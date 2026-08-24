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
pub const PROJECT_CURRENT_STATE_OPERATION: &str = "project.current_state";
pub const PROJECT_CHARTER_ADOPTION_OPERATION: &str = "project.charter.adoption";
pub const PROJECT_DOCUMENT_OPERATION: &str = "project.document";
pub const PROJECT_DECISION_OPERATION: &str = "project.decision";
pub const PROJECT_EXECUTION_BASELINE_OPERATION: &str = "project.execution_baseline";
pub const PROJECT_MILESTONE_OPERATION: &str = "project.milestone";
pub const PROJECT_EVIDENCE_OPERATION: &str = "project.evidence";
pub const PROJECT_READINESS_OPERATION: &str = "project.readiness";
pub const PROJECT_RELEASE_OPERATION: &str = "project.release.request";
pub const TASK_PROPOSE_OPERATION: &str = "task.propose";
pub const TASK_ADAPTIVE_OPERATION: &str = "task.adaptive";

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
    CoordinationEnvelope,
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

/// The canonical permission family. The concrete persisted permission name
/// is selected by `permission_for_scope`, so the same operation can safely be
/// used by an Account/Agent Chat or Project/Project Chat scope without a
/// second operation declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationPermission {
    ReadAccountOrAgentChat,
    ReadAccount,
    ReadAgentChat,
    ReadProjectOrAgentChat,
    ReadProject,
    ProposeDiscovery,
    ProposeProject,
    ProposeTask,
}

impl OperationPermission {
    #[must_use]
    pub const fn for_scope(self, scope_type: CanonicalScopeType) -> Option<&'static str> {
        match (self, scope_type) {
            (Self::ReadAccountOrAgentChat, CanonicalScopeType::Account) => Some("read_account"),
            (Self::ReadAccountOrAgentChat, CanonicalScopeType::AgentChat) => {
                Some("read_agent_chat")
            }
            (Self::ReadAccount, CanonicalScopeType::Account) => Some("read_account"),
            (Self::ReadAgentChat, CanonicalScopeType::AgentChat) => Some("read_agent_chat"),
            (Self::ReadProjectOrAgentChat, CanonicalScopeType::Project) => Some("read_project"),
            (Self::ReadProjectOrAgentChat, CanonicalScopeType::AgentChat) => {
                Some("read_agent_chat")
            }
            (Self::ReadProject, CanonicalScopeType::Project) => Some("read_project"),
            (
                Self::ProposeDiscovery,
                CanonicalScopeType::Account | CanonicalScopeType::AgentChat,
            ) => Some("propose_discovery"),
            (
                Self::ProposeProject,
                CanonicalScopeType::Account
                | CanonicalScopeType::Project
                | CanonicalScopeType::AgentChat,
            ) => Some("propose_project"),
            (Self::ProposeTask, CanonicalScopeType::Project | CanonicalScopeType::AgentChat) => {
                Some("propose_task")
            }
            _ => None,
        }
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

const MAIN_SCOPES: &[CanonicalScopeType] =
    &[CanonicalScopeType::Account, CanonicalScopeType::AgentChat];
const PROJECT_SCOPES: &[CanonicalScopeType] =
    &[CanonicalScopeType::Project, CanonicalScopeType::AgentChat];

pub const MIGRATED_OPERATION_CONTRACTS: &[OperationContract] = &[
    OperationContract {
        operation: MAIN_GENESIS_START_OPERATION,
        surface: OperationSurface::MainOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::Always,
        supported_scopes: MAIN_SCOPES,
        classification: OperationClassification::DirectCommand,
        permission: OperationPermission::ProposeDiscovery,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION,
        surface: OperationSurface::MainOrchestration,
        exposure: OperationExposure::TypedRead,
        input: OperationInputContract::ReadArguments,
        setup: OperationSetupExposure::Always,
        supported_scopes: MAIN_SCOPES,
        classification: OperationClassification::Query,
        permission: OperationPermission::ReadAccountOrAgentChat,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
        surface: OperationSurface::MainOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::Always,
        supported_scopes: MAIN_SCOPES,
        classification: OperationClassification::DirectCommand,
        permission: OperationPermission::ProposeDiscovery,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: MAIN_CHARTER_READ_OPERATION,
        surface: OperationSurface::MainOrchestration,
        exposure: OperationExposure::TypedRead,
        input: OperationInputContract::ReadArguments,
        setup: OperationSetupExposure::Always,
        supported_scopes: MAIN_SCOPES,
        classification: OperationClassification::Query,
        permission: OperationPermission::ReadAccountOrAgentChat,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: MAIN_CHARTER_DRAFT_OPERATION,
        surface: OperationSurface::MainOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::Always,
        supported_scopes: MAIN_SCOPES,
        classification: OperationClassification::DirectCommand,
        permission: OperationPermission::ProposeDiscovery,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: MAIN_CHARTER_READINESS_OPERATION,
        surface: OperationSurface::MainOrchestration,
        exposure: OperationExposure::TypedRead,
        input: OperationInputContract::ReadArguments,
        setup: OperationSetupExposure::Always,
        supported_scopes: MAIN_SCOPES,
        classification: OperationClassification::Query,
        permission: OperationPermission::ReadAccountOrAgentChat,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: MAIN_CHARTER_DIFF_OPERATION,
        surface: OperationSurface::MainOrchestration,
        exposure: OperationExposure::TypedRead,
        input: OperationInputContract::ReadArguments,
        setup: OperationSetupExposure::Always,
        supported_scopes: MAIN_SCOPES,
        classification: OperationClassification::Query,
        permission: OperationPermission::ReadAccountOrAgentChat,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
        surface: OperationSurface::MainOrchestration,
        exposure: OperationExposure::TypedRead,
        input: OperationInputContract::ReadArguments,
        setup: OperationSetupExposure::Always,
        supported_scopes: MAIN_SCOPES,
        classification: OperationClassification::Query,
        permission: OperationPermission::ReadAccountOrAgentChat,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: MAIN_PROJECT_CREATE_OPERATION,
        surface: OperationSurface::MainOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::Always,
        supported_scopes: MAIN_SCOPES,
        classification: OperationClassification::ApprovalRequiredAction,
        permission: OperationPermission::ProposeProject,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: PROJECT_CURRENT_STATE_OPERATION,
        surface: OperationSurface::ProjectOrchestration,
        exposure: OperationExposure::TypedRead,
        input: OperationInputContract::ReadArguments,
        setup: OperationSetupExposure::Always,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::Query,
        permission: OperationPermission::ReadProjectOrAgentChat,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: PROJECT_CHARTER_ADOPTION_OPERATION,
        surface: OperationSurface::ProjectOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::SetupOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::DirectCommand,
        permission: OperationPermission::ProposeProject,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: PROJECT_DOCUMENT_OPERATION,
        surface: OperationSurface::ProjectOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::ReadyOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::ApprovalRequiredAction,
        permission: OperationPermission::ProposeProject,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: PROJECT_DECISION_OPERATION,
        surface: OperationSurface::ProjectOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::ReadyOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::ApprovalRequiredAction,
        permission: OperationPermission::ProposeProject,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: PROJECT_EXECUTION_BASELINE_OPERATION,
        surface: OperationSurface::ProjectOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::ReadyOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::ApprovalRequiredAction,
        permission: OperationPermission::ProposeProject,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: PROJECT_MILESTONE_OPERATION,
        surface: OperationSurface::ProjectOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::ReadyOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::ApprovalRequiredAction,
        permission: OperationPermission::ProposeProject,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: PROJECT_EVIDENCE_OPERATION,
        surface: OperationSurface::ProjectOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::ReadyOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::ApprovalRequiredAction,
        permission: OperationPermission::ProposeProject,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: PROJECT_READINESS_OPERATION,
        surface: OperationSurface::ProjectOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::ReadyOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::ApprovalRequiredAction,
        permission: OperationPermission::ProposeProject,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: PROJECT_RELEASE_OPERATION,
        surface: OperationSurface::ProjectOrchestration,
        exposure: OperationExposure::TypedProposal,
        input: OperationInputContract::ProposalEnvelope,
        setup: OperationSetupExposure::ReadyOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::ApprovalRequiredAction,
        permission: OperationPermission::ProposeProject,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: TASK_PROPOSE_OPERATION,
        surface: OperationSurface::Coordination,
        exposure: OperationExposure::GenericProposal,
        input: OperationInputContract::CoordinationEnvelope,
        setup: OperationSetupExposure::ReadyOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::DirectCommand,
        permission: OperationPermission::ProposeTask,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
    OperationContract {
        operation: TASK_ADAPTIVE_OPERATION,
        surface: OperationSurface::Coordination,
        exposure: OperationExposure::GenericProposal,
        input: OperationInputContract::CoordinationEnvelope,
        setup: OperationSetupExposure::ReadyOnly,
        supported_scopes: PROJECT_SCOPES,
        classification: OperationClassification::DirectCommand,
        permission: OperationPermission::ProposeTask,
        output: SHARED_ORCHESTRATION_OUTCOME,
    },
];

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
    if let Some(permission) = operation_contract_permission(scope_type, operation) {
        return Some(permission);
    }
    let permission = match (scope_type, operation) {
        (
            CanonicalScopeType::Account,
            "account.summary" | "discovery.read" | "portfolio.read" | "inbox.read"
            | "commitments.read" | "delivery.read",
        ) => "read_account",
        (
            CanonicalScopeType::Project,
            "project.summary" | "work.read" | "events.read" | "inbox.read" | "commitments.read"
            | "delivery.read",
        ) => "read_project",
        (
            CanonicalScopeType::AgentChat,
            "agent_chat.summary" | "discovery.read" | "portfolio.read" | "project.summary"
            | "events.read" | "inbox.read" | "commitments.read" | "delivery.read",
        ) => "read_agent_chat",
        (
            CanonicalScopeType::Task,
            "task.summary" | "work.read" | "events.read" | "inbox.read" | "commitments.read"
            | "delivery.read",
        ) => "read_task",
        (_, "memory.read" | "decisions.read") => "read_memory",
        (_, "message.propose" | "message.send") => "propose_message",
        (_, "commitment.propose" | "commitment.update") => "propose_commitment",
        (_, "memory.publish" | "memory.supersede") => "propose_memory",
        (_, "review.propose" | "review.request") => "propose_review",
        (_, "session.action") => "propose_session",
        _ => return None,
    };
    Some(permission)
}

#[must_use]
pub fn is_query_operation(operation: &str) -> bool {
    if let Some(contract) = operation_contract(operation) {
        return contract.classification == OperationClassification::Query;
    }
    matches!(
        operation,
        "web.search"
            | "account.summary"
            | "project.summary"
            | "agent_chat.summary"
            | "task.summary"
            | "discovery.read"
            | "portfolio.read"
            | "work.read"
            | "decisions.read"
            | "events.read"
            | "memory.read"
            | "inbox.read"
            | "commitments.read"
            | "delivery.read"
    )
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
    if let Some(contract) = operation_contract(operation) {
        return contract.classification == OperationClassification::ApprovalRequiredAction;
    }
    matches!(
        operation,
        "message.propose"
            | "message.send"
            | "commitment.propose"
            | "commitment.update"
            | "memory.publish"
            | "memory.supersede"
            | "review.propose"
            | "review.request"
            | "session.action"
    )
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
    let action = payload.get("action").and_then(Value::as_str);
    match operation {
        PROJECT_CHARTER_ADOPTION_OPERATION => action == Some("draft_revision"),
        PROJECT_DOCUMENT_OPERATION => matches!(
            action,
            Some("draft_revision") | Some("propose_approval") | Some("approve")
        ),
        PROJECT_DECISION_OPERATION => {
            matches!(action, Some("record_candidate") | Some("record_effective"))
                && payload.get("decision_class").and_then(Value::as_str)
                    == Some("project_implementation")
        }
        PROJECT_EXECUTION_BASELINE_OPERATION => matches!(
            action,
            Some("draft_revision") | Some("revise") | Some("propose_approval")
        ),
        PROJECT_MILESTONE_OPERATION => {
            matches!(
                action,
                Some("define") | Some("revise") | Some("set_primary")
            )
        }
        PROJECT_EVIDENCE_OPERATION => action == Some("attach"),
        PROJECT_READINESS_OPERATION => action == Some("evaluate"),
        // A release candidate is a consequential approval/audit proposal even
        // though it does not perform the final immutable release itself.  It
        // must retain an AgentAction until the user-facing approval contract
        // consumes it.
        PROJECT_RELEASE_OPERATION => false,
        _ => false,
    }
}

/// Return whether a payload attempts to supply authority or scope that Forge
/// must derive from the authenticated binding.
#[must_use]
pub fn contains_authority_override(value: &Value) -> bool {
    const FORBIDDEN_FIELDS: &[&str] = &[
        "actor_identity_id",
        "identity_id",
        "scope_type",
        "scope_id",
        "project_id",
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
    match value {
        Value::Object(object) => object.iter().any(|(key, nested)| {
            FORBIDDEN_FIELDS.contains(&key.as_str()) || contains_authority_override(nested)
        }),
        Value::Array(values) => values.iter().any(contains_authority_override),
        _ => false,
    }
}

/// Return whether an adaptive task payload attempts to provide a value that
/// Forge must derive from the authenticated Project/Project-chat binding or
/// the active execution governance.  These fields are operation-specific:
/// they are intentionally not part of the generic authority list because
/// some older proposal payloads still carry their own domain fields.
#[must_use]
pub fn contains_adaptive_authority_override(value: &Value) -> bool {
    const FORBIDDEN_FIELDS: &[&str] = &[
        "actor",
        "actor_id",
        "actor_identity_id",
        "authority",
        "baseline_id",
        "baseline_revision_id",
        "charter_revision_id",
        "credential",
        "elevated_operations",
        "executor",
        "fixed_acceptance",
        "fixed_boundary",
        "fixed_boundary_digest",
        "fixed_outcomes",
        "fixed_risk_classes",
        "forbidden_side_effects",
        "governance",
        "identity_id",
        "permission",
        "plan_item_id",
        "project_id",
        "provenance",
        "release_policy_digest",
        "release_policy_revision",
        "repository_path",
        "repository_url",
        "risk_class",
        "scope_id",
        "scope_type",
        "target_id",
        "target_type",
        "workspace",
        "workspace_lease",
        "workspace_path",
    ];

    match value {
        Value::Object(object) => object.iter().any(|(key, nested)| {
            FORBIDDEN_FIELDS.contains(&key.as_str()) || contains_adaptive_authority_override(nested)
        }),
        Value::Array(values) => values.iter().any(contains_adaptive_authority_override),
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
    match operation {
        // Generic operation names are only used by the built-in catalog.
        "account.summary" => "account.summary",
        "project.summary" => "project.summary",
        "agent_chat.summary" => "agent_chat.summary",
        "task.summary" => "task.summary",
        "discovery.read" => "discovery.read",
        "portfolio.read" => "portfolio.read",
        "work.read" => "work.read",
        "decisions.read" => "decisions.read",
        "events.read" => "events.read",
        "memory.read" => "memory.read",
        "inbox.read" => "inbox.read",
        "commitments.read" => "commitments.read",
        "delivery.read" => "delivery.read",
        "web.search" => "web.search",
        TASK_PROPOSE_OPERATION => TASK_PROPOSE_OPERATION,
        "message.propose" => "message.propose",
        "message.send" => "message.send",
        "commitment.propose" => "commitment.propose",
        "commitment.update" => "commitment.update",
        "memory.publish" => "memory.publish",
        "memory.supersede" => "memory.supersede",
        "review.propose" => "review.propose",
        "review.request" => "review.request",
        "session.action" => "session.action",
        "project.lifecycle" => "project.lifecycle",
        "handoff.publish" => "handoff.publish",
        "decision.request" => "decision.request",
        "project.release" => "project.release",
        "project.milestone.release" => "project.milestone.release",
        "waiver.create" => "waiver.create",
        "release.execute" => "release.execute",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn migrated_contracts_have_unique_complete_metadata() {
        let mut operations = BTreeSet::new();

        for contract in MIGRATED_OPERATION_CONTRACTS {
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
    fn project_subaction_classification_is_payload_aware() {
        let direct = serde_json::json!({"action":"draft_revision"});
        let protected = serde_json::json!({"action":"activate"});
        assert_eq!(
            classify_operation(PROJECT_EXECUTION_BASELINE_OPERATION, Some(&direct)),
            OperationClassification::DirectCommand
        );
        assert_eq!(
            classify_operation(PROJECT_EXECUTION_BASELINE_OPERATION, Some(&protected)),
            OperationClassification::ApprovalRequiredAction
        );
        assert_eq!(
            classify_operation(PROJECT_EXECUTION_BASELINE_OPERATION, None),
            OperationClassification::ApprovalRequiredAction
        );
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
        assert_eq!(contract.input, OperationInputContract::CoordinationEnvelope);
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
    fn adaptive_task_rejects_scope_and_governance_overrides() {
        let payload = serde_json::json!({
            "action": "split",
            "source_task_id": "task-1",
            "expected_task_version": 1,
            "expected_board_revision": 1,
            "rationale": "decompose",
            "items": [{"title": "child"}],
            "project_id": "other-project",
            "governance": {"fixed_boundary_digest": "forged"}
        });
        assert!(contains_adaptive_authority_override(&payload));
    }
}
