//! Canonical catalog for Forge native operation exposure and admission.
//!
//! The catalog is intentionally owned by the host crate, below the runtime
//! descriptor layer and above the service adapters.  It is the one place that
//! answers the transport-neutral question "what kind of operation is this?":
//! queries are read-only, direct commands already have an atomic command
//! service, approval-required operations retain an `AgentAction` envelope,
//! and denied operations are absent from a safe descriptor and rejected again
//! at the service boundary.

use serde_json::Value;
use std::sync::LazyLock;

use crate::CanonicalScopeType;

pub const MAIN_CHARTER_READ_OPERATION: &str = "charter.read";
pub const MAIN_GENESIS_START_OPERATION: &str = "genesis.start";
pub const MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION: &str = "genesis.project_agents.read";
pub const MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION: &str = "genesis.project_agent.select";
pub const MAIN_CHARTER_DRAFT_OPERATION: &str = "charter.draft";
pub const MAIN_CHARTER_READINESS_OPERATION: &str = "charter.readiness";
pub const MAIN_CHARTER_DIFF_OPERATION: &str = "charter.diff";
pub const MAIN_CHARTER_APPROVAL_TARGET_OPERATION: &str = "charter.approval_target";
pub const MAIN_PROJECT_CREATE_OPERATION: &str = "project.create";
/// Dispatch one ephemeral read-only sub-agent to answer a bounded question,
/// and return its findings to the calling turn.
///
/// This is deliberately not a Task: there is no worktree, no workflow state,
/// no review, and no dispatch queue. One nested turn runs with the account
/// read surface and an account scratch directory, its transcript never enters
/// the caller's history, and only a bounded abstract comes back. It exists so
/// Main can spend a research excursion's context somewhere other than the
/// conversation that has to survive all day.
pub const MAIN_INQUIRY_RUN_OPERATION: &str = "inquiry.run";
pub const PROJECT_CURRENT_STATE_OPERATION: &str = "project.current_state";
/// Read what a Task run actually reported: its worklog entries and the
/// artifacts it captured. This is how a Project Agent checks an observation
/// before citing it, and how it decides whether the outcome needs a fix Task.
pub const PROJECT_OBSERVATIONS_OPERATION: &str = "project.observations";
pub const REPO_INTEGRATION_QUEUE_OPERATION: &str = "repo.integration_queue";
/// Read the current approved Charter's full rendered text on demand. The
/// Charter is Project data, not resident prompt content: the handoff packet
/// and the operating skill carry only its identity and digests, so this read
/// is how the Agent gets the actual words whenever a decision needs them.
pub const PROJECT_CHARTER_READ_OPERATION: &str = "project.charter";
/// Read one server-owned Project doctrine section by name. The resident
/// operating skill keeps only an index of these sections so the detailed
/// doctrine stops occupying every turn's fixed prompt budget.
pub const PROJECT_SKILL_SECTION_OPERATION: &str = "skill.section";
/// The canonical doctrine section names, owned by the `skill.section`
/// operation spec. The services provider must serve exactly these names; a
/// drift between the two is caught by a services-side test.
pub const PROJECT_SKILL_SECTION_NAMES: &[&str] = operation_registry::project_reads::SECTION_NAMES;
pub const PROJECT_CHARTER_ADOPTION_OPERATION: &str = "project.charter.adoption";
/// Replace the Project's default independent review commands while
/// preserving every other Project review-policy field.
pub const PROJECT_REVIEW_CONFIG_OPERATION: &str = "project.review_config";
pub const PROJECT_DOCUMENT_OPERATION: &str = "project.document";
pub const PROJECT_DECISION_OPERATION: &str = "project.decision";
pub const PROJECT_MILESTONE_OPERATION: &str = "project.milestone";
pub const PROJECT_EVIDENCE_OPERATION: &str = "project.evidence";
pub const PROJECT_VALIDATION_OPERATION: &str = "project.validation";
pub const PROJECT_ESCALATE_OPERATION: &str = "project.escalate";
pub const PROJECT_READINESS_OPERATION: &str = "project.readiness";
pub const PROJECT_RELEASE_OPERATION: &str = "project.release.request";
pub const TASK_PROPOSE_OPERATION: &str = "task.propose";
pub const TASK_ADAPTIVE_OPERATION: &str = "task.adaptive";
/// Add or remove one prerequisite edge between two Tasks in the bound
/// Project.
///
/// Without this an Agent could only set edges at `task.propose` and never
/// change one, so re-planning meant cancelling every downstream Task and
/// recreating it under a new id — while the same capability was already
/// reachable over REST.
pub const TASK_DEPENDENCY_OPERATION: &str = "task.dependency";
/// Repair a Task that stopped, instead of replacing it. A transient failure --
/// an expired lease, a lost runtime, a dispatch race -- leaves a perfectly good
/// Task stranded; recovering it is what the Project Agent's own protocol asks
/// for, and without this the only reachable remedy is a duplicate Task.
pub const TASK_ACTION_OPERATION: &str = "task.action";
/// Write the current execution's plan candidate. Forge publishes the
/// candidate only after the execution completes successfully; this operation
/// never edits the Task's canonical plan directly.
pub const TASK_PLAN_OPERATION: &str = "task.plan";
/// Capture one artifact a Task run actually produced as authoritative
/// evidence. This lives in the Task scope on purpose: only a Task session has
/// a workspace and a process to observe, so it is the only place an
/// observation can be recorded rather than described.
pub const TASK_EVIDENCE_OPERATION: &str = "task.evidence";
/// Append one provenance-bearing entry to the Task worklog. This is narration
/// the next role reads, never workflow truth: it moves no Task, satisfies no
/// acceptance check, and is not milestone evidence.
pub const TASK_WORKLOG_OPERATION: &str = "task.worklog";

/// The typed native surface that owns an operation's public contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationSurface {
    MainOrchestration,
    ProjectOrchestration,
    Coordination,
}

/// How a canonical operation is exposed to a native Agent Runtime model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationExposure {
    TypedRead,
    TypedProposal,
    GenericProposal,
}

/// The transport-neutral input envelope advertised for an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationInputContract {
    ReadArguments,
    ProposalEnvelope,
}

/// Whether an operation is available in a legacy Project setup session, a
/// ready Project session, or both. The setup distinction is host-derived and
/// never model-controlled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationSetupExposure {
    Always,
    SetupOnly,
    ReadyOnly,
}

/// Permission projection of the registry's authority rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationPermission(pub operation_registry::AuthorityRule);
impl OperationPermission {
    #[must_use]
    pub fn admits_permission(self, permission: &str) -> bool {
        self.0
            .permissions
            .iter()
            .any(|(_, name)| *name == permission)
    }
    pub fn for_scope(self, scope: CanonicalScopeType) -> Option<&'static str> {
        self.0.permission(scope_name(scope))
    }
}
fn scope_name(scope: CanonicalScopeType) -> &'static str {
    match scope {
        CanonicalScopeType::Account => "account",
        CanonicalScopeType::Project => "project",
        CanonicalScopeType::AgentChat => "agent_chat",
        CanonicalScopeType::Task => "task",
    }
}

/// Output metadata shared by every migrated native orchestration operation.
/// The result is always an `api_types::OrchestrationOutcome`; replay is a
/// field on the envelope and never a second status/classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperationOutputContract {
    pub envelope: &'static str,
    pub in_band_errors: bool,
    pub replay_field: &'static str,
}

pub const SHARED_ORCHESTRATION_OUTCOME: OperationOutputContract = OperationOutputContract {
    envelope: "api_types::OrchestrationOutcome",
    in_band_errors: true,
    replay_field: "replayed",
};

/// One canonical declaration for a migrated native operation. JSON input
/// schemas are generated from this declaration in `operation_contract`; the
/// service remains responsible for domain/business validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationContract {
    pub operation: &'static str,
    pub surface: OperationSurface,
    pub exposure: OperationExposure,
    pub input: OperationInputContract,
    pub setup: OperationSetupExposure,
    pub supported_scopes: &'static [CanonicalScopeType],
    pub classification: OperationClassification,
    pub permission: OperationPermission,
    pub output: OperationOutputContract,
}

#[cfg(test)]
const TASK_SCOPES: &[CanonicalScopeType] = &[CanonicalScopeType::Task];

/// Generated host metadata. Input, permission, effect and availability are
/// projected from the registry; the host adds only its outcome envelope.
pub static MIGRATED_OPERATION_CONTRACTS: LazyLock<Vec<OperationContract>> = LazyLock::new(|| {
    operation_registry::READ_CATALOG
        .iter()
        .chain(operation_registry::PROPOSAL_CATALOG.iter())
        .filter_map(|spec| {
            let aggregate = spec.surfaces.first()?.native_aggregate;
            let (surface, exposure, input) = match aggregate {
                "forge_main_orchestration_read" => (
                    OperationSurface::MainOrchestration,
                    OperationExposure::TypedRead,
                    OperationInputContract::ReadArguments,
                ),
                "forge_main_orchestration_propose" => (
                    OperationSurface::MainOrchestration,
                    OperationExposure::TypedProposal,
                    OperationInputContract::ProposalEnvelope,
                ),
                "forge_project_orchestration_read" => (
                    OperationSurface::ProjectOrchestration,
                    OperationExposure::TypedRead,
                    OperationInputContract::ReadArguments,
                ),
                "forge_project_orchestration_propose" => (
                    OperationSurface::ProjectOrchestration,
                    OperationExposure::TypedProposal,
                    OperationInputContract::ProposalEnvelope,
                ),
                "forge_scope_propose"
                    if spec.effect == operation_registry::EffectClass::DirectCommand =>
                {
                    (
                        OperationSurface::Coordination,
                        if spec.authority.permission("task").is_some() {
                            OperationExposure::TypedProposal
                        } else {
                            OperationExposure::GenericProposal
                        },
                        OperationInputContract::ProposalEnvelope,
                    )
                }
                _ => return None,
            };
            let scopes = spec
                .authority
                .permissions
                .iter()
                .map(|(scope, _)| match *scope {
                    "account" => CanonicalScopeType::Account,
                    "project" => CanonicalScopeType::Project,
                    "agent_chat" => CanonicalScopeType::AgentChat,
                    "task" => CanonicalScopeType::Task,
                    _ => unreachable!("native registry scope"),
                })
                .collect::<Vec<_>>();
            Some(OperationContract {
                operation: spec.id,
                surface,
                exposure,
                input,
                setup: match spec.availability {
                    operation_registry::AvailabilityRule::Always
                    | operation_registry::AvailabilityRule::MainChatOnly => {
                        OperationSetupExposure::Always
                    }
                    operation_registry::AvailabilityRule::SetupOnly => {
                        OperationSetupExposure::SetupOnly
                    }
                    operation_registry::AvailabilityRule::ReadyOnly => {
                        OperationSetupExposure::ReadyOnly
                    }
                },
                supported_scopes: Box::leak(scopes.into_boxed_slice()),
                classification: effect_classification(spec.effect),
                permission: OperationPermission(spec.authority),
                output: SHARED_ORCHESTRATION_OUTCOME,
            })
        })
        .collect::<Vec<_>>()
});
fn effect_classification(effect: operation_registry::EffectClass) -> OperationClassification {
    match effect {
        operation_registry::EffectClass::Query => OperationClassification::Query,
        operation_registry::EffectClass::DirectCommand => OperationClassification::DirectCommand,
        operation_registry::EffectClass::ApprovalRequired => {
            OperationClassification::ApprovalRequiredAction
        }
    }
}

#[must_use]
pub fn operation_contract(operation: &str) -> Option<&'static OperationContract> {
    MIGRATED_OPERATION_CONTRACTS
        .iter()
        .find(|contract| contract.operation == operation)
}

#[must_use]
pub fn operation_supported_in_scope(operation: &str, scope_type: CanonicalScopeType) -> bool {
    operation_contract(operation)
        .is_some_and(|contract| contract.supported_scopes.contains(&scope_type))
}

#[must_use]
pub fn operation_contract_permission(
    scope_type: CanonicalScopeType,
    operation: &str,
) -> Option<&'static str> {
    operation_contract(operation).and_then(|contract| contract.permission.for_scope(scope_type))
}

#[must_use]
pub fn operation_names_for_surface(
    surface: OperationSurface,
    project_charter_setup_required: bool,
    exposure: OperationExposure,
) -> Vec<String> {
    MIGRATED_OPERATION_CONTRACTS
        .iter()
        .filter(|contract| {
            contract.surface == surface
                && contract.exposure == exposure
                && match contract.setup {
                    OperationSetupExposure::Always => true,
                    OperationSetupExposure::SetupOnly => project_charter_setup_required,
                    OperationSetupExposure::ReadyOnly => !project_charter_setup_required,
                }
        })
        .map(|contract| contract.operation.to_owned())
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationClassification {
    Query,
    DirectCommand,
    ApprovalRequiredAction,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationDescriptor {
    pub operation: &'static str,
    pub classification: OperationClassification,
    pub required_permission: Option<&'static str>,
}

impl OperationDescriptor {
    #[must_use]
    pub const fn is_exposed(self) -> bool {
        !matches!(self.classification, OperationClassification::Denied)
    }

    #[must_use]
    pub const fn is_query(self) -> bool {
        matches!(self.classification, OperationClassification::Query)
    }

    #[must_use]
    pub const fn is_direct_command(self) -> bool {
        matches!(self.classification, OperationClassification::DirectCommand)
    }

    #[must_use]
    pub const fn is_approval_required(self) -> bool {
        matches!(
            self.classification,
            OperationClassification::ApprovalRequiredAction
        )
    }
}

/// Classify a native Forge operation using the optional, already-parsed
/// operation payload.  Dynamic Project operations are conservatively treated
/// as approval-required when no payload is available (for example while a
/// descriptor is being built); the service boundary calls this function with
/// the payload and can admit only the closed direct-safe subaction set.
#[must_use]
pub fn classify_operation(operation: &str, payload: Option<&Value>) -> OperationClassification {
    if let Some(contract) = operation_contract(operation) {
        if is_project_orchestration_operation(operation) {
            return if payload
                .is_some_and(|payload| is_allowed_project_direct_payload(operation, payload))
            {
                OperationClassification::DirectCommand
            } else {
                OperationClassification::ApprovalRequiredAction
            };
        }
        return contract.classification;
    }
    if let Some(spec) = operation_registry::READ_CATALOG
        .lookup(operation)
        .or_else(|| operation_registry::PROPOSAL_CATALOG.lookup(operation))
    {
        return effect_classification(spec.effect);
    }
    if is_query_operation(operation) {
        return OperationClassification::Query;
    }
    if is_denied_operation(operation) {
        return OperationClassification::Denied;
    }
    if is_approval_required_operation(operation) {
        return OperationClassification::ApprovalRequiredAction;
    }
    OperationClassification::Denied
}

/// Return the canonical descriptor for an operation.  Unknown operations are
/// represented as denied descriptors so callers can safely filter without
/// first maintaining a second allowlist.
#[must_use]
pub fn descriptor(
    scope_type: CanonicalScopeType,
    operation: &str,
    payload: Option<&Value>,
) -> OperationDescriptor {
    OperationDescriptor {
        operation: leaked_operation_name(operation),
        classification: classify_operation(operation, payload),
        required_permission: operation_permission(scope_type, operation),
    }
}

/// Resolve the persisted Forge permission ceiling for one public native
/// operation.  A missing permission deliberately returns `None`; descriptor
/// composition must omit such an operation rather than grant a guessed
/// ceiling.
#[must_use]
pub fn operation_permission(
    scope_type: CanonicalScopeType,
    operation: &str,
) -> Option<&'static str> {
    if let Some(spec) = operation_registry::READ_CATALOG
        .lookup(operation)
        .or_else(|| operation_registry::PROPOSAL_CATALOG.lookup(operation))
    {
        return spec.authority.permission(match scope_type {
            CanonicalScopeType::Account => "account",
            CanonicalScopeType::Project => "project",
            CanonicalScopeType::AgentChat => "agent_chat",
            CanonicalScopeType::Task => "task",
        });
    }
    if let Some(permission) = operation_contract_permission(scope_type, operation) {
        return Some(permission);
    }
    None
}

/// Whether `operation` is a coordination command an Agent may execute
/// directly under `requested_permission`.
///
/// This used to be a hand-written list of operation names in the policy
/// layer, and its own comment recorded the cost: `task.recover` was declared,
/// implemented end to end, and omitted from that list, so it was refused at
/// runtime with a message that named nothing. The catalog row already carries
/// every fact the decision needs — a coordination surface, the generic
/// proposal exposure, a direct classification, and the permission — so ask it
/// instead. Typed-proposal coordination operations (`task.plan`,
/// `task.worklog`, `task.evidence`) execute through their own dedicated path
/// and are deliberately not admitted here.
#[must_use]
pub fn is_coordination_direct_command(operation: &str, requested_permission: &str) -> bool {
    is_coordination_generic_proposal(operation)
        && operation_contract(operation)
            .is_some_and(|contract| contract.permission.admits_permission(requested_permission))
}

/// Whether `operation` rides the generic coordination proposal envelope.
///
/// This is the set the flat provider-facing payload surface has to serve: one
/// envelope shared by every such operation, so a field any of them requires
/// must be declared there or a provider that surfaces only declared
/// properties will strip it.
#[must_use]
pub fn is_coordination_generic_proposal(operation: &str) -> bool {
    operation_contract(operation).is_some_and(|contract| {
        contract.surface == OperationSurface::Coordination
            && contract.exposure == OperationExposure::GenericProposal
            && contract.classification == OperationClassification::DirectCommand
    })
}

#[must_use]
pub fn is_query_operation(operation: &str) -> bool {
    operation_registry::READ_CATALOG.lookup(operation).is_some() || operation == "web.search"
}

#[must_use]
pub fn is_denied_operation(operation: &str) -> bool {
    matches!(
        operation,
        "project.lifecycle"
            | "handoff.publish"
            | "decision.request"
            | "project.release"
            | "project.milestone.release"
            | "waiver.create"
            | "release.execute"
    )
}

#[must_use]
pub fn is_approval_required_operation(operation: &str) -> bool {
    operation_registry::PROPOSAL_CATALOG
        .lookup(operation)
        .is_some_and(|spec| spec.effect == operation_registry::EffectClass::ApprovalRequired)
}

#[must_use]
pub fn is_project_orchestration_operation(operation: &str) -> bool {
    operation_contract(operation).is_some_and(|contract| {
        contract.surface == OperationSurface::ProjectOrchestration
            && contract.exposure == OperationExposure::TypedProposal
    })
}

/// The closed direct-safe Project subaction set.  This is deliberately
/// payload-aware: the same coarse operation descriptor can contain an
/// approval-required authority action, while only the reversible bounded
/// subactions below may bypass the Action queue.
#[must_use]
pub fn is_allowed_project_direct_payload(operation: &str, payload: &Value) -> bool {
    if contains_authority_override(payload) {
        return false;
    }
    if let Some(spec) = operation_registry::PROPOSAL_CATALOG.lookup(operation) {
        if spec.effect != operation_registry::EffectClass::DirectCommand {
            return false;
        }
        let properties = &spec.input.schema["properties"];
        if let Some(action_schema) = properties.get("action") {
            let Some(action) = payload.get("action") else {
                return false;
            };
            if let Some(values) = action_schema["enum"].as_array() {
                if !values.contains(action) {
                    return false;
                }
            } else if (action_schema["type"] == "object" || action_schema["oneOf"].is_array())
                && serde_json::from_value::<api_types::TaskAction>(action.clone()).is_err()
            {
                return false;
            }
        }
        if let Some(values) = properties["decision_class"]["enum"].as_array() {
            if !payload
                .get("decision_class")
                .is_some_and(|value| values.contains(value))
            {
                return false;
            }
        }
        if properties.get("action").is_none() {
            return spec.validate_arguments(payload).is_ok();
        }
        return true;
    }
    false
}

/// Field names whose values Forge derives from the authenticated binding.
/// Every authority guard starts from this one list: the dispatch guard below
/// adds `project_id`, and native preparation adds its prompt-injection names
/// (`typed_tools::reject_authority_overrides`).
pub(crate) const SERVER_DERIVED_FIELDS: &[&str] = &[
    "actor_identity_id",
    "identity_id",
    "scope_type",
    "scope_id",
    "authority",
    "permission",
    "workspace",
    "workspace_path",
    "workspace_lease",
    "repository_path",
    "repository_url",
    "credential",
    "target_type",
    "target_id",
];

/// Return whether a payload attempts to supply authority or scope that Forge
/// must derive from the authenticated binding.
#[must_use]
pub fn contains_authority_override(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, nested)| {
            key == "project_id"
                || SERVER_DERIVED_FIELDS.contains(&key.as_str())
                || contains_authority_override(nested)
        }),
        Value::Array(values) => values.iter().any(contains_authority_override),
        _ => false,
    }
}

// `OperationDescriptor` intentionally carries a static operation label so a
// descriptor can be copied into an immutable tool registry.  Unknown names
// are denied before this helper is normally observed; leaking a caller-owned
// string here would require an allocation and would weaken that invariant.
fn leaked_operation_name(operation: &str) -> &'static str {
    if let Some(contract) = operation_contract(operation) {
        return contract.operation;
    }
    if let Some(spec) = operation_registry::READ_CATALOG
        .lookup(operation)
        .or_else(|| operation_registry::PROPOSAL_CATALOG.lookup(operation))
    {
        return spec.id;
    }
    match operation {
        "web.search" => "web.search",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn direct_command_admission_is_derived_and_still_excludes_typed_task_proposals() {
        // The set the policy layer used to hand-list. Pinning it here means a
        // new coordination command is admitted the moment its catalog row
        // exists, while a change that would widen admission by accident — a
        // typed-proposal operation switching exposure, say — fails loudly.
        let admitted: Vec<&str> = MIGRATED_OPERATION_CONTRACTS
            .iter()
            .map(|contract| contract.operation)
            .filter(|operation| is_coordination_direct_command(operation, "propose_task"))
            .collect();
        assert_eq!(
            admitted,
            vec![
                TASK_ACTION_OPERATION,
                TASK_ADAPTIVE_OPERATION,
                TASK_DEPENDENCY_OPERATION,
                TASK_PROPOSE_OPERATION,
            ]
        );

        // These are coordination direct commands as well, but they run
        // through their own Task-scoped proposal path and must not be
        // admitted through the Project coordination envelope.
        for typed in [
            TASK_PLAN_OPERATION,
            TASK_WORKLOG_OPERATION,
            TASK_EVIDENCE_OPERATION,
        ] {
            assert!(!is_coordination_direct_command(typed, "propose_task"));
            assert!(!is_coordination_direct_command(typed, "task_read"));
        }

        // The permission is part of the decision, not decoration.
        assert!(!is_coordination_direct_command(
            TASK_ACTION_OPERATION,
            "read_project"
        ));
        assert!(!is_coordination_direct_command(
            "task.invented",
            "propose_task"
        ));
    }

    #[test]
    fn migrated_contracts_have_unique_complete_metadata() {
        let mut operations = BTreeSet::new();

        for contract in MIGRATED_OPERATION_CONTRACTS.iter() {
            assert!(
                operations.insert(contract.operation),
                "duplicate migrated operation contract: {}",
                contract.operation
            );
            assert!(!contract.operation.is_empty());
            assert!(!contract.supported_scopes.is_empty());
            assert_eq!(contract.output, SHARED_ORCHESTRATION_OUTCOME);
            assert_eq!(contract.output.envelope, "api_types::OrchestrationOutcome");
            assert!(contract.output.in_band_errors);
            assert_eq!(contract.output.replay_field, "replayed");

            for scope_type in contract.supported_scopes {
                assert!(operation_supported_in_scope(
                    contract.operation,
                    *scope_type
                ));
                assert_eq!(
                    operation_contract_permission(*scope_type, contract.operation),
                    contract.permission.for_scope(*scope_type),
                    "permission metadata diverged for {} in {scope_type:?}",
                    contract.operation
                );
                assert!(contract.permission.for_scope(*scope_type).is_some());
            }
        }
    }

    #[test]
    fn catalog_has_one_class_for_the_four_operation_families() {
        assert_eq!(
            classify_operation("project.summary", None),
            OperationClassification::Query
        );
        assert_eq!(
            classify_operation(MAIN_CHARTER_DRAFT_OPERATION, None),
            OperationClassification::DirectCommand
        );
        assert_eq!(
            classify_operation(MAIN_GENESIS_START_OPERATION, None),
            OperationClassification::DirectCommand
        );
        assert_eq!(
            classify_operation(MAIN_PROJECT_CREATE_OPERATION, None),
            OperationClassification::ApprovalRequiredAction
        );
        assert_eq!(
            classify_operation(TASK_ADAPTIVE_OPERATION, None),
            OperationClassification::DirectCommand
        );
        assert_eq!(
            classify_operation("project.lifecycle", None),
            OperationClassification::Denied
        );
    }

    #[test]
    fn genesis_start_is_main_only_and_uses_discovery_permission() {
        for scope in [CanonicalScopeType::Account, CanonicalScopeType::AgentChat] {
            assert!(operation_supported_in_scope(
                MAIN_GENESIS_START_OPERATION,
                scope
            ));
            assert_eq!(
                operation_permission(scope, MAIN_GENESIS_START_OPERATION),
                Some("propose_discovery")
            );
        }
        assert!(!operation_supported_in_scope(
            MAIN_GENESIS_START_OPERATION,
            CanonicalScopeType::Project
        ));
        assert!(!operation_supported_in_scope(
            MAIN_GENESIS_START_OPERATION,
            CanonicalScopeType::Task
        ));
    }

    #[test]
    fn authority_fields_cannot_turn_a_project_subaction_into_a_direct_command() {
        let payload = serde_json::json!({
            "action":"record_effective",
            "decision_class":"project_implementation",
            "project_id":"other-project"
        });
        assert_eq!(
            classify_operation(PROJECT_DECISION_OPERATION, Some(&payload)),
            OperationClassification::ApprovalRequiredAction
        );
    }

    #[test]
    fn project_review_config_is_a_ready_project_direct_command() {
        let contract = operation_contract(PROJECT_REVIEW_CONFIG_OPERATION)
            .expect("Project review-config contract");
        assert_eq!(contract.surface, OperationSurface::ProjectOrchestration);
        assert_eq!(contract.exposure, OperationExposure::TypedProposal);
        assert_eq!(contract.setup, OperationSetupExposure::ReadyOnly);
        assert_eq!(
            contract.permission.for_scope(CanonicalScopeType::Project),
            Some("propose_project")
        );
        assert_eq!(
            classify_operation(
                PROJECT_REVIEW_CONFIG_OPERATION,
                Some(&serde_json::json!({
                    "action": "set_ci_steps",
                    "expected_project_version": 3,
                    "ci_steps": ["cargo test --workspace"]
                }))
            ),
            OperationClassification::DirectCommand
        );
    }

    #[test]
    fn unknown_operations_are_denied_and_have_no_permission() {
        let descriptor = descriptor(CanonicalScopeType::Project, "made_up.operation", None);
        assert_eq!(descriptor.classification, OperationClassification::Denied);
        assert_eq!(descriptor.required_permission, None);
        assert!(!descriptor.is_exposed());
    }

    #[test]
    fn adaptive_task_is_ready_only_project_scoped_and_uses_task_permission() {
        let contract = operation_contract(TASK_ADAPTIVE_OPERATION).expect("adaptive contract");
        assert_eq!(contract.surface, OperationSurface::Coordination);
        assert_eq!(contract.exposure, OperationExposure::GenericProposal);
        assert_eq!(contract.input, OperationInputContract::ProposalEnvelope);
        assert_eq!(contract.setup, OperationSetupExposure::ReadyOnly);
        assert_eq!(
            contract.classification,
            OperationClassification::DirectCommand
        );
        assert_eq!(
            operation_contract_permission(CanonicalScopeType::Project, TASK_ADAPTIVE_OPERATION),
            Some("propose_task")
        );
        assert_eq!(
            operation_contract_permission(CanonicalScopeType::AgentChat, TASK_ADAPTIVE_OPERATION),
            Some("propose_task")
        );
        assert_eq!(
            operation_contract_permission(CanonicalScopeType::Account, TASK_ADAPTIVE_OPERATION),
            None
        );
        assert!(
            operation_names_for_surface(
                OperationSurface::Coordination,
                false,
                OperationExposure::GenericProposal,
            )
            .iter()
            .any(|operation| operation == TASK_ADAPTIVE_OPERATION)
        );
        assert!(
            !operation_names_for_surface(
                OperationSurface::Coordination,
                true,
                OperationExposure::GenericProposal,
            )
            .iter()
            .any(|operation| operation == TASK_ADAPTIVE_OPERATION)
        );
    }

    #[test]
    fn task_plan_is_a_task_scoped_typed_direct_command() {
        let contract = operation_contract(TASK_PLAN_OPERATION).expect("Task plan contract");
        assert_eq!(contract.surface, OperationSurface::Coordination);
        assert_eq!(contract.exposure, OperationExposure::TypedProposal);
        assert_eq!(contract.input, OperationInputContract::ProposalEnvelope);
        assert_eq!(contract.setup, OperationSetupExposure::Always);
        assert_eq!(contract.supported_scopes, TASK_SCOPES);
        assert_eq!(
            contract.classification,
            OperationClassification::DirectCommand
        );
        assert_eq!(
            contract.permission.for_scope(CanonicalScopeType::Task),
            Some("task_read")
        );
        assert_eq!(
            operation_contract_permission(CanonicalScopeType::Task, TASK_PLAN_OPERATION),
            Some("task_read")
        );
        assert!(
            operation_names_for_surface(
                OperationSurface::Coordination,
                false,
                OperationExposure::TypedProposal,
            )
            .iter()
            .any(|operation| operation == TASK_PLAN_OPERATION)
        );
        assert!(
            operation_names_for_surface(
                OperationSurface::Coordination,
                true,
                OperationExposure::TypedProposal,
            )
            .iter()
            .any(|operation| operation == TASK_PLAN_OPERATION)
        );
        for scope in [
            CanonicalScopeType::Account,
            CanonicalScopeType::Project,
            CanonicalScopeType::AgentChat,
        ] {
            assert!(!operation_supported_in_scope(TASK_PLAN_OPERATION, scope));
            assert_eq!(
                operation_contract_permission(scope, TASK_PLAN_OPERATION),
                None
            );
        }
    }
}
