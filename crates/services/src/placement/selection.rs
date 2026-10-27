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
use sqlx::{Sqlite, Transaction};

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
    /// What the filesystem holding this owner's workspace root is short of,
    /// by its last reading. `None` also when there is no reading.
    pub disk_pressure: Option<api_types::DiskPressureKind>,
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
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
                                | PlacementFilterCode::DiskPressure
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
                            | DiskPressure
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
                        | PlacementFilterCode::DiskPressure
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
                        | PlacementFilterCode::DiskPressure
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
            .is_none_or(|revision| revision < api_types::DAEMON_MIN_PROTOCOL_REVISION)
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
            .is_none_or(|capacity| !capacity.admits_execution())
    {
        filters.insert(MachineCapacity);
    }
    // Under its floor a machine starts no new worktree. A Task that already
    // has a ready worktree there (its own, or the root's it shares) is never
    // refused: finishing work is how space comes back.
    if !candidate.provisioning
        && candidate.disk_pressure.is_some()
        && !context.binding().is_some_and(|binding| {
            binding.state == PlacementState::Ready && matches_placement(candidate, binding)
        })
    {
        filters.insert(DiskPressure);
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
    }) || filters.iter().all(|code| {
        matches!(
            code,
            LocationNotReady | AgentCapacity | MachineCapacity | DiskPressure
        )
    }) {
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
    let rows = db
        .placement_repository_location_sources_in_tx(transaction, &input.repo.id)
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
        let runtimes = db
            .placement_provision_runtime_sources_in_tx(
                transaction,
                &input.repo.id,
                db.server_run_cap.embedded_machine_id(),
            )
            .await?;
        for runtime in runtimes {
            let id: String = runtime.id()?;
            let daemon: String = runtime.daemon_id()?;
            let retry_exists = runtime.retry_runtime()?.is_some();
            let retry_location: Option<String> = runtime.retry_location()?;
            let exhausted = super::provisioning::ProvisionRetryState {
                attempts: runtime.retry_attempts()?.unwrap_or_default(),
                inputs_digest: runtime.retry_digest()?.unwrap_or_default(),
                connection_id: runtime.retry_connection()?.unwrap_or_default(),
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
                path: std::path::Path::new(&runtime.workspace_root()?)
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
                created_at: runtime.created_at()?,
                updated_at: runtime.updated_at()?,
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
            Some(id) => {
                db.placement_execution_daemon_source_in_tx(transaction, id)
                    .await?
            }
            None => None,
        };
        let embedded_execution = !daemon_owned
            && (execution_daemon_id.is_none()
                || daemon.as_ref().is_some_and(|row| {
                    row.machine_id()
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
                (Some(runtime_id), Some(daemon_id)) => db
                    .placement_runtime_ready_in_tx(transaction, runtime_id, daemon_id)
                    .await?
                    .unwrap_or(false),
                _ => false,
            }
        } else {
            true
        };
        let visible = match daemon.as_ref() {
            Some(row) => {
                let owner: Option<String> = row.owner_id()?;
                let visibility: String = row.visibility()?;
                visibility == "global" || owner.is_none() || owner.as_deref() == input.task_owner_id
            }
            None => !remote_execution && execution_daemon_id.is_none(),
        };
        let detected: serde_json::Value = daemon
            .as_ref()
            .map(|row| row.detected_clis_json())
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
                        Some(owner) => db
                            .placement_owned_cli_enabled_in_tx(
                                transaction,
                                owner,
                                daemon_id,
                                &agent.executor_type,
                            )
                            .await?
                            .unwrap_or(true),
                        None => db
                            .placement_unowned_cli_enabled_in_tx(
                                transaction,
                                daemon_id,
                                &agent.executor_type,
                            )
                            .await?
                            .unwrap_or(true),
                    };
                    facts.enabled &= enabled;
                }
            }
            executors.insert(agent.id.clone(), facts);
        }
        let machine_id = if execution_daemon_id.is_none()
            || daemon.as_ref().is_some_and(|row| {
                row.machine_id()
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
                .map(|row| row.max_concurrent_runs())
                .transpose()?
                .flatten();
            let admin = daemon
                .as_ref()
                .map(|row| row.run_limit())
                .transpose()?
                .flatten();
            super::capacity::effective_machine_cap(reported, admin)
        };
        let embedded_machine_id = db.server_run_cap.embedded_machine_id();
        let machine_capacity =
            count_machine_capacity(transaction, machine_id, cap, &embedded_machine_id).await?;
        // An aged run-slot waiter is admitted ahead of the checks queued
        // since it began to wait (`db::machine_capacity::capacity_for_task`).
        let machine_capacity = Some(
            db::machine_capacity::capacity_for_task(
                transaction,
                machine_capacity,
                machine_id,
                &input.task.id,
                &embedded_machine_id,
            )
            .await?,
        );
        // The worktree lives on its owner's disk: the server's root for a
        // server-owned location (wherever it executes), the daemon's
        // reported root otherwise.
        let disk_pressure = if daemon_owned {
            let disk_json = daemon
                .as_ref()
                .map(|row| row.disk_json())
                .transpose()?
                .flatten();
            db.disk_admission.daemon_pressure(disk_json.as_deref())
        } else {
            db.disk_admission.server_pressure()
        };
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
            disk_pressure,
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

fn map_location(row: db::PlacementRepoLocationSourceRow) -> Result<RepoLocation> {
    Ok(RepoLocation {
        id: row.id()?,
        repo_id: row.repo_id()?,
        owner_kind: row.owner_kind()?.parse().map_err(db::DbError::Check)?,
        daemon_id: row.daemon_id()?,
        runtime_id: row.runtime_id()?,
        path: row.path()?,
        kind: row.kind()?.parse().map_err(db::DbError::Check)?,
        is_default: row.is_default()? != 0,
        status: row.status()?.parse().map_err(db::DbError::Check)?,
        last_verified_at: row.last_verified_at()?,
        last_error: row.last_error()?,
        version: row.version()?,
        created_at: row.created_at()?,
        updated_at: row.updated_at()?,
    })
}

#[cfg(test)]
mod tests;
