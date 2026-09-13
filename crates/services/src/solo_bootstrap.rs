//! Repository-scoped bootstrap for Forge Solo.
//!
//! Solo owns a single, isolated SQLite store.  This service is the only
//! boundary the TUI should use to materialize that store: it creates the
//! internal owner, the Project shell, the local repository binding, and the
//! canonical Project Chat/setup binding.  Charter adoption is intentionally
//! *not* performed here.  A Project remains setup-required until the existing
//! Charter command service consumes an explicit approval action.
//!
//! The service is deliberately conservative.  SQLite has a composite for
//! Project + Chat + setup-binding creation, but there is no existing
//! composite spanning that operation, owner membership, and Repo creation.
//! Those typed repository calls are therefore ordered and reconciled on every
//! retry.  The Solo process lock is the concurrency boundary for the normal
//! runtime; a mismatch is always reported rather than guessed around.

use std::{collections::HashSet, path::Path, sync::Arc};

use api_types::canonical_digest_with_schema;
use db::{
    validate_uuid_v4, Agent, AgentChat, AgentChatRepo, AgentListQuery, AgentProfileRepo, AgentRepo,
    AgentStatus, CreateAgent, CreateProject, CreateProjectMember, CreateRepo, Page, PageRequest,
    Project, ProjectAdmissionReceiptRepo, ProjectAgentBinding, ProjectAgentBindingRepo,
    ProjectMember, ProjectMemberRepo, ProjectRepo, Repo, RepoRepo, SortBy, SortOrder, SqliteDb,
    SystemSettingRepo, UpdateProject, UpdateRepo, User, UserRepo, WorkMode,
};
use executors::ExecutorKind;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{
    agent_service::{cli_runtime_source_enabled, compute_effective_status, EffectiveStatus},
    workflow::{default_autonomous_workflow::default_autonomous_workflow, default_roles},
    Result, ServiceError,
};

/// Durable bootstrap contract.  Changing this value intentionally changes
/// the idempotency domain and the Project settings schema.
pub const SOLO_BOOTSTRAP_CONTRACT_REVISION: &str = "forge.solo.bootstrap/v1";

/// The built-in workflow selected for every new Solo Project.
pub const SOLO_WORKFLOW_TEMPLATE_NAME: &str = "autonomous_v1";

/// Project settings key containing the immutable Solo binding metadata.
pub const SOLO_PROJECT_SETTINGS_KEY: &str = "forge_solo";

const SOLO_OWNER_SETTING_PREFIX: &str = "forge.solo.owner.";
const SOLO_AGENT_REGISTRATION_KEY: &str = "forge_solo_registration_key";
const PROJECT_AGENT_BINDING_SETUP_STATE: &str = "agent_setup_required";
const PROJECT_CHAT_SETUP_STATUS: &str = "agent_setup_required";
const PROJECT_CHAT_READY_STATUS: &str = "ready";
// Persist the Project Agent's post-adoption ceiling while the Charter gate is
// still closed. The canonical Agent Chat scope intersects this ceiling with
// `project.charter_setup_required`, so Task and other operational proposals
// remain unavailable until the approval transaction commits. Keeping the
// future ceiling here lets that transaction preserve a deliberately narrowed
// binding for ordinary Projects without leaving Solo permanently setup-only.
const SOLO_PROJECT_PERMISSION_CEILING_JSON: &str = r#"{"permissions":["read_project","read_agent_chat","read_task","read_memory","propose_task","propose_project","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session"]}"#;
const SOLO_PROJECT_NAME_MAX_CHARS: usize = 200;
const MAX_RECONCILIATION_PAGES: usize = 1024;

/// Availability reported by the structured local-harness discovery layer.
/// Only `Authenticated` may enter the eligible candidate set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloAgentAvailability {
    Authenticated,
    Installed,
    NotFound,
    Unauthenticated,
    Disabled,
    Unknown,
}

impl SoloAgentAvailability {
    fn is_authenticated(self) -> bool {
        matches!(self, Self::Authenticated)
    }
}

/// Health observed while reconciling one candidate against its current Agent,
/// Profile, credential, daemon, and effective source state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloAgentHealth {
    Healthy,
    Busy,
    Unavailable,
}

/// Adapter/daemon discovery input supplied by the local runtime.  The
/// identity and profile IDs are opaque durable IDs; availability is never
/// inferred from executable names or free-form output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloAgentCandidateInput {
    pub identity_id: String,
    pub profile_id: Option<String>,
    pub executor_type: String,
    pub availability: SoloAgentAvailability,
    #[serde(default)]
    pub display_name: Option<String>,
}

impl SoloAgentCandidateInput {
    /// Construct the smallest candidate record produced by a discovery
    /// adapter.  The profile revision is re-read from the Agent boundary.
    #[must_use]
    pub fn authenticated(identity_id: impl Into<String>, executor_type: impl Into<String>) -> Self {
        Self {
            identity_id: identity_id.into(),
            profile_id: None,
            executor_type: executor_type.into(),
            availability: SoloAgentAvailability::Authenticated,
            display_name: None,
        }
    }
}

/// A typed registration descriptor for a harness discovered before its
/// owner-scoped Agent identity exists in this isolated store. The adapter
/// must supply `Authenticated`; presence of an executable is not enough.
/// `registration_key` is persisted in the Agent config as a replay anchor so
/// a response lost after the Agent/Profile composite commits cannot create a
/// second identity on retry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloAuthenticatedAgentRegistration {
    pub registration_key: String,
    pub name: String,
    pub executor_type: String,
    pub daemon_id: Option<String>,
    pub credential_ref: Option<String>,
    pub availability: SoloAgentAvailability,
}

impl SoloAuthenticatedAgentRegistration {
    /// Construct a registration descriptor from a positive structured
    /// discovery result. The service still rechecks the source and Agent
    /// health before returning a candidate.
    #[must_use]
    pub fn authenticated(
        registration_key: impl Into<String>,
        name: impl Into<String>,
        executor_type: impl Into<String>,
        daemon_id: Option<String>,
    ) -> Self {
        Self {
            registration_key: registration_key.into(),
            name: name.into(),
            executor_type: executor_type.into(),
            daemon_id,
            credential_ref: None,
            availability: SoloAgentAvailability::Authenticated,
        }
    }
}

/// A presentation-neutral candidate returned by bootstrap.  The `eligible`
/// bit is derived by the service; callers must not turn an ineligible row into
/// a binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloAgentCandidate {
    pub identity_id: String,
    pub profile_id: Option<String>,
    pub display_name: String,
    pub executor_type: String,
    pub availability: SoloAgentAvailability,
    pub health: SoloAgentHealth,
    pub eligible: bool,
    pub reason: Option<String>,
    pub next_step: Option<String>,
}

/// Inputs for one idempotent bootstrap/reconciliation attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloBootstrapRequest {
    /// UUID persisted in the Git common directory marker.
    pub repository_id: String,
    /// Stable repository identity supplied by the Git resolver.  This is not
    /// derived from a display name and is part of the immutable digest.
    pub canonical_repository: String,
    /// Exact source checkout path registered in the local Repo row.  For the
    /// initial integration this may equal `canonical_repository`; keeping it
    /// distinct lets the Git resolver preserve identity when a checkout moves.
    #[serde(default)]
    pub source_path: Option<String>,
    /// Exact isolated Solo data root.  The service does not fall back to a
    /// different store when this value changes.
    pub data_root: String,
    pub default_branch: String,
    /// Optional display name.  It is not part of the repository identity and
    /// may change when a user renames a checkout.
    #[serde(default)]
    pub repository_name: Option<String>,
    /// Structured local-harness candidates observed by the caller.  When
    /// empty, the service discovers current owned Agent rows and rechecks
    /// their source state through existing service/repository boundaries.
    #[serde(default)]
    pub agent_candidates: Vec<SoloAgentCandidateInput>,
    /// A confirmed Project Agent identity.  Omitting it leaves setup-required
    /// state and returns the bounded candidate list; no fallback is chosen.
    #[serde(default)]
    pub selected_project_agent_id: Option<String>,
    /// A confirmed Task Worker identity.  This is independent from the
    /// Project Agent selection.  Reusing the same identity is accepted only
    /// when this field explicitly names it and its policy contains
    /// `task_write`.
    #[serde(default)]
    pub selected_worker_agent_id: Option<String>,
}

impl SoloBootstrapRequest {
    /// Minimal request helper for callers whose Git resolver uses one stable
    /// identity/path value.
    #[must_use]
    pub fn new(
        repository_id: impl Into<String>,
        canonical_repository: impl Into<String>,
        data_root: impl Into<String>,
        default_branch: impl Into<String>,
    ) -> Self {
        Self {
            repository_id: repository_id.into(),
            canonical_repository: canonical_repository.into(),
            source_path: None,
            data_root: data_root.into(),
            default_branch: default_branch.into(),
            repository_name: None,
            agent_candidates: Vec::new(),
            selected_project_agent_id: None,
            selected_worker_agent_id: None,
        }
    }

    fn source_path(&self) -> &str {
        self.source_path
            .as_deref()
            .unwrap_or(self.canonical_repository.as_str())
    }
}

/// The durable stage that the TUI should render after bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoloBootstrapReadiness {
    AgentSelectionRequired,
    WorkerSelectionRequired,
    CharterAdoptionRequired,
    Ready,
}

/// Canonical outcome for both first launch and every restart/retry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoloBootstrapResult {
    pub idempotency_key: String,
    pub input_digest: String,
    pub repository_id: String,
    pub canonical_repository: String,
    pub data_root: String,
    pub owner_id: String,
    pub project_id: String,
    pub repo_id: String,
    pub project_chat_id: String,
    pub project_agent_binding_id: String,
    pub project_agent_identity_id: Option<String>,
    pub project_agent_profile_id: Option<String>,
    /// Confirmed setup choice, if any. Unlike the binding fields above this
    /// remains populated while the Project is waiting for Charter adoption.
    pub selected_project_agent_id: Option<String>,
    pub worker_identity_id: Option<String>,
    pub selected_worker_agent_id: Option<String>,
    pub workflow_template_name: String,
    pub readiness: SoloBootstrapReadiness,
    pub agent_candidates: Vec<SoloAgentCandidate>,
    pub suggested_project_agent_id: Option<String>,
    pub adoption_required: bool,
    pub mutation_authority_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SoloOwnerBinding {
    contract_revision: String,
    repository_id: String,
    canonical_repository: String,
    data_root: String,
    owner_id: String,
}

/// Immutable metadata stored below [`SOLO_PROJECT_SETTINGS_KEY`]. Agent
/// selections are setup choices, not authority. Once a Project Agent is
/// confirmed, the legacy setup binding may carry its identity so the existing
/// Agent Chat admission path can conduct adoption; `charter_setup_required`
/// and the least-privilege ceiling still keep Task mutation gated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SoloProjectMetadata {
    contract_revision: String,
    repository_id: String,
    canonical_repository: String,
    data_root: String,
    default_branch: String,
    idempotency_key: String,
    input_digest: String,
    owner_id: String,
    workflow_template_name: String,
    #[serde(default)]
    selected_project_agent_id: Option<String>,
    #[serde(default)]
    selected_worker_agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SoloBootstrapDigestInput<'a> {
    repository_id: &'a str,
    canonical_repository: &'a str,
    data_root: &'a str,
    default_branch: &'a str,
}

#[derive(Debug, Clone)]
struct CandidateEvaluation {
    candidate: SoloAgentCandidate,
    agent: Option<Agent>,
}

/// Typed, idempotent Solo bootstrap service.
#[derive(Clone)]
pub struct SoloBootstrapService {
    db: Arc<SqliteDb>,
}

impl std::fmt::Debug for SoloBootstrapService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SoloBootstrapService")
            .finish_non_exhaustive()
    }
}

impl SoloBootstrapService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Create or resume the one Solo owner/Project/repository/chat setup for
    /// the request's immutable repository identity.
    pub async fn bootstrap(&self, request: SoloBootstrapRequest) -> Result<SoloBootstrapResult> {
        let request = normalize_request(request)?;
        let (idempotency_key, input_digest) = bootstrap_identity(&request)?;
        let workflow = default_autonomous_workflow();
        crate::workflow::validation::validate_workflow(&workflow)?;
        let workflow_json = serde_json::to_string(&workflow).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "autonomous_v1 workflow is not serializable: {error}"
            ))
        })?;

        // Inspect all Projects before creating an owner.  An isolated store
        // containing an unbound/foreign Project must fail closed without
        // exposing or mutating that Project through a new Solo owner.
        let projects = list_projects(&self.db).await?;
        let existing_project_metadata =
            inspect_project_scope(&projects, &request, &idempotency_key, &input_digest)?;
        let owner_id = self
            .ensure_owner(&request, existing_project_metadata.as_ref())
            .await?;

        if let Some(metadata) = existing_project_metadata.as_ref() {
            if metadata.owner_id != owner_id {
                return Err(conflict("Solo owner does not match the Project owner"));
            }
        }

        let mut evaluations = self
            .candidate_evaluations(&request, &owner_id, existing_project_metadata.as_ref())
            .await?;
        let mut metadata = existing_project_metadata.unwrap_or_else(|| SoloProjectMetadata {
            contract_revision: SOLO_BOOTSTRAP_CONTRACT_REVISION.to_owned(),
            repository_id: request.repository_id.clone(),
            canonical_repository: request.canonical_repository.clone(),
            data_root: request.data_root.clone(),
            default_branch: request.default_branch.clone(),
            idempotency_key: idempotency_key.clone(),
            input_digest: input_digest.clone(),
            owner_id: owner_id.clone(),
            workflow_template_name: SOLO_WORKFLOW_TEMPLATE_NAME.to_owned(),
            selected_project_agent_id: None,
            selected_worker_agent_id: None,
        });

        reconcile_requested_selection(&mut metadata, &request, &evaluations, &owner_id)?;

        // Validate a confirmed selection before materializing any new
        // Project state.  A later final check closes the small window between
        // this read and the metadata CAS below.
        self.require_selected_candidates(&metadata, &evaluations, false)
            .await?;

        let mut project = if let Some(existing) = projects.into_iter().next() {
            let metadata_from_project = parse_project_metadata(&existing)?;
            if metadata_from_project != metadata {
                // Selection fields are the only setup fields allowed to move
                // after first materialization.  Immutable fields were already
                // checked by inspect_project_scope.
                let mut expected = metadata_from_project.clone();
                expected.selected_project_agent_id = metadata.selected_project_agent_id.clone();
                expected.selected_worker_agent_id = metadata.selected_worker_agent_id.clone();
                if expected != metadata {
                    return Err(conflict(
                        "Solo Project metadata changed outside the bootstrap boundary",
                    ));
                }
            }
            verify_project_workflow(&existing, &workflow)?;
            if existing.owner_id.as_deref() != Some(owner_id.as_str()) {
                return Err(conflict("Solo Project owner mismatch"));
            }
            existing
        } else {
            let project_name = project_name(&request);
            let settings =
                project_settings(&metadata, metadata.selected_worker_agent_id.as_deref())?;
            let now = db::now_rfc3339();
            ProjectRepo::create(
                &*self.db,
                CreateProject {
                    id: db::new_uuid_v4(),
                    name: project_name,
                    settings,
                    workflow_definition: workflow_json.clone(),
                    primary_repo_id: None,
                    owner_id: Some(owner_id.clone()),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await?
        };

        // Persist a newly confirmed selection through the Project settings
        // CAS.  This is setup metadata only; it never activates the binding.
        project = self.persist_selection_metadata(project, &metadata).await?;

        self.ensure_owner_membership(&project, &owner_id).await?;
        let repo = self.ensure_local_repo(&mut project, &request).await?;
        project = ProjectRepo::get_by_id(&*self.db, &project.id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project.id.clone()))?;

        let mut chat = self.ensure_chat(&project).await?;
        let mut binding = self.ensure_binding(&project, &chat, &metadata).await?;

        // A confirmed Project Agent is allowed to conduct the legacy Charter
        // adoption conversation. This is a binding replacement, not Charter
        // approval: the replacement remains `charter_setup_required`, and the
        // canonical scope gate withholds its latent operational permissions.
        // Recheck the selected source immediately before this authority-bearing
        // binding write; no alternate identity is ever substituted.
        if project.charter_status == "legacy_unverified"
            && project.charter_setup_required
            && metadata.selected_project_agent_id.is_some()
            && binding.state == PROJECT_AGENT_BINDING_SETUP_STATE
        {
            evaluations = self
                .candidate_evaluations(&request, &owner_id, Some(&metadata))
                .await?;
            self.require_selected_candidates(&metadata, &evaluations, true)
                .await?;
            self.activate_project_agent_for_adoption(&project, &metadata, &binding)
                .await?;
            chat = self.ensure_chat(&project).await?;
            binding = self.ensure_binding(&project, &chat, &metadata).await?;
        }

        // Final revalidation is deliberately after all durable setup rows and
        // immediately before the result is returned.  If the selected source
        // went away, setup remains intact and no alternate identity is used.
        evaluations = self
            .candidate_evaluations(&request, &owner_id, Some(&metadata))
            .await?;
        self.require_selected_candidates(&metadata, &evaluations, true)
            .await?;

        let result = self
            .result(
                &request,
                (&idempotency_key, &input_digest),
                &metadata,
                (&project, &repo, &chat, &binding),
                evaluations,
            )
            .await?;
        Ok(result)
    }

    /// List current candidate state for a scoped owner without materializing
    /// a Project.  This is useful for a retryable first-run picker.
    pub async fn list_candidates(
        &self,
        owner_id: &str,
        observed: &[SoloAgentCandidateInput],
    ) -> Result<Vec<SoloAgentCandidate>> {
        let owner_id = required("owner_id", owner_id)?;
        let inputs = if observed.is_empty() {
            self.discovered_candidate_inputs(owner_id).await?
        } else {
            observed.to_vec()
        };
        Ok(self
            .evaluate_candidate_inputs(owner_id, inputs)
            .await?
            .into_iter()
            .map(|evaluation| evaluation.candidate)
            .collect())
    }

    /// Create or resume an owner-scoped local CLI Agent from a positive,
    /// structured harness discovery result. This is the convergent bridge for
    /// a new isolated store, where no Agent row exists yet. It deliberately
    /// does not claim an ownerless/global Agent: the current typed Agent
    /// boundary has no safe owner-CAS operation, and silently taking one would
    /// cross account scope. The returned candidate may still be ineligible if
    /// the daemon/source changed between registration and the health read.
    pub async fn ensure_authenticated_agent(
        &self,
        owner_id: &str,
        mut registration: SoloAuthenticatedAgentRegistration,
    ) -> Result<SoloAgentCandidate> {
        let owner_id = required("owner_id", owner_id)?;
        UserRepo::get_user_by_id(&*self.db, owner_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("user", owner_id.to_owned()))?;
        registration.registration_key = registration.registration_key.trim().to_owned();
        registration.name = registration.name.trim().to_owned();
        registration.executor_type = registration.executor_type.trim().to_ascii_lowercase();
        if registration.registration_key.is_empty() {
            return Err(ServiceError::invalid_operation(
                "registration_key must not be empty",
            ));
        }
        if registration.name.is_empty() {
            return Err(ServiceError::invalid_operation("name must not be empty"));
        }
        if !registration.availability.is_authenticated() {
            return Err(ServiceError::invalid_operation(
                "only authenticated harness discovery may register a Solo Agent",
            ));
        }
        if !allowed_cli_executor(&registration.executor_type) {
            return Err(ServiceError::invalid_operation(format!(
                "unsupported Solo CLI executor: {}",
                registration.executor_type
            )));
        }
        if registration
            .daemon_id
            .as_deref()
            .is_some_and(|daemon_id| daemon_id.trim().is_empty())
        {
            registration.daemon_id = None;
        }
        if registration
            .credential_ref
            .as_deref()
            .is_some_and(|credential_ref| credential_ref.trim().is_empty())
        {
            registration.credential_ref = None;
        }

        let agents = list_agents(&self.db).await?;
        let matching = agents
            .iter()
            .filter(|agent| {
                agent_registration_key(agent).as_deref()
                    == Some(registration.registration_key.as_str())
            })
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return Err(conflict(
                "more than one Agent is bound to the same Solo registration key",
            ));
        }

        let agent = if let Some(agent) = matching.into_iter().next() {
            if agent.owner_id.as_deref() != Some(owner_id) {
                return Err(conflict(
                    "Solo registration key is already bound to another owner scope",
                ));
            }
            if agent.executor_type != registration.executor_type
                || agent.daemon_id != registration.daemon_id
                || agent.credential_ref != registration.credential_ref
            {
                return Err(conflict(
                    "Solo Agent registration changed its executor or source binding",
                ));
            }
            agent.clone()
        } else {
            let now = db::now_rfc3339();
            let config_json = serde_json::to_string(&json!({
                SOLO_AGENT_REGISTRATION_KEY: registration.registration_key,
            }))
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "Solo Agent registration metadata is not serializable: {error}"
                ))
            })?;
            AgentRepo::create(
                &*self.db,
                CreateAgent {
                    id: db::new_uuid_v4(),
                    name: registration.name.clone(),
                    description: Some("Forge Solo local CLI harness".to_owned()),
                    executor_type: registration.executor_type.clone(),
                    model: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    prompt_template: None,
                    capabilities_json: "[]".to_owned(),
                    config_json,
                    credential_ref: registration.credential_ref.clone(),
                    daemon_id: registration.daemon_id.clone(),
                    max_concurrent_tasks: 1,
                    heartbeat_interval_seconds: 30,
                    max_missed_heartbeats: 3,
                    status: AgentStatus::Idle,
                    last_heartbeat_at: Some(now.clone()),
                    is_default: false,
                    paused: false,
                    owner_id: Some(owner_id.to_owned()),
                    visibility: "account".to_owned(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await?
        };

        let evaluation = self
            .evaluate_candidate(
                owner_id,
                SoloAgentCandidateInput {
                    identity_id: agent.id,
                    profile_id: Some(agent.profile_id),
                    executor_type: registration.executor_type,
                    availability: registration.availability,
                    display_name: Some(registration.name),
                },
            )
            .await?;
        Ok(evaluation.candidate)
    }

    async fn ensure_owner(
        &self,
        request: &SoloBootstrapRequest,
        existing_metadata: Option<&SoloProjectMetadata>,
    ) -> Result<String> {
        let key = owner_setting_key(&request.repository_id);
        if let Some(value) = SystemSettingRepo::get_setting(&*self.db, &key).await? {
            let binding = serde_json::from_str::<SoloOwnerBinding>(&value)
                .map_err(|error| conflict(format!("Solo owner setting is malformed: {error}")))?;
            verify_owner_binding(&binding, request)?;
            let user = UserRepo::get_user_by_id(&*self.db, &binding.owner_id)
                .await?
                .ok_or_else(|| conflict("Solo owner setting points to a missing user"))?;
            if user.id != binding.owner_id {
                return Err(conflict("Solo owner setting resolved to the wrong user"));
            }
            verify_internal_owner(&user, &request.repository_id)?;
            return Ok(binding.owner_id);
        }

        // A response can be lost after Project creation but before writing the
        // setting.  Project metadata is authoritative for recovering that
        // owner; creating a second principal would break scope identity.
        let owner_id = if let Some(metadata) = existing_metadata {
            let user = UserRepo::get_user_by_id(&*self.db, &metadata.owner_id)
                .await?
                .ok_or_else(|| conflict("Solo Project owner is missing from the user table"))?;
            if user.id != metadata.owner_id {
                return Err(conflict("Solo Project owner metadata is invalid"));
            }
            verify_internal_owner(&user, &request.repository_id)?;
            metadata.owner_id.clone()
        } else {
            let email = owner_email(&request.repository_id);
            if let Some(user) = UserRepo::get_user_by_email(&*self.db, &email).await? {
                user.id
            } else {
                let id = db::new_uuid_v4();
                let now = db::now_rfc3339();
                // This is deliberately not a password hash.  The owner is an
                // internal, non-login principal; the random value is retained
                // only to satisfy the legacy User schema and is never returned
                // or logged.
                UserRepo::create_user(
                    &*self.db,
                    &User {
                        id: id.clone(),
                        email,
                        password_hash: format!("forge-solo-unrecoverable:{}", db::new_uuid_v4()),
                        display_name: Some("Forge Solo owner".to_owned()),
                        is_admin: false,
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await?;
                id
            }
        };

        let owner = UserRepo::get_user_by_id(&*self.db, &owner_id)
            .await?
            .ok_or_else(|| conflict("Solo owner principal disappeared during bootstrap"))?;
        verify_internal_owner(&owner, &request.repository_id)?;

        let binding = SoloOwnerBinding {
            contract_revision: SOLO_BOOTSTRAP_CONTRACT_REVISION.to_owned(),
            repository_id: request.repository_id.clone(),
            canonical_repository: request.canonical_repository.clone(),
            data_root: request.data_root.clone(),
            owner_id: owner_id.clone(),
        };
        let value = serde_json::to_string(&binding).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "Solo owner setting is not serializable: {error}"
            ))
        })?;
        SystemSettingRepo::set_setting(&*self.db, &key, &value, &db::now_rfc3339()).await?;
        Ok(owner_id)
    }

    async fn candidate_evaluations(
        &self,
        request: &SoloBootstrapRequest,
        owner_id: &str,
        durable_metadata: Option<&SoloProjectMetadata>,
    ) -> Result<Vec<CandidateEvaluation>> {
        let mut inputs = if request.agent_candidates.is_empty() {
            self.discovered_candidate_inputs(owner_id).await?
        } else {
            request.agent_candidates.clone()
        };

        // A persisted selection must still be re-readable even when a caller
        // retries without passing the previous discovery response.
        for identity_id in [
            request.selected_project_agent_id.as_deref(),
            request.selected_worker_agent_id.as_deref(),
            durable_metadata.and_then(|metadata| metadata.selected_project_agent_id.as_deref()),
            durable_metadata.and_then(|metadata| metadata.selected_worker_agent_id.as_deref()),
        ]
        .into_iter()
        .flatten()
        {
            if !inputs.iter().any(|input| input.identity_id == identity_id) {
                if let Some(agent) = AgentRepo::get_by_id(&*self.db, identity_id).await? {
                    inputs.push(SoloAgentCandidateInput {
                        identity_id: agent.id,
                        profile_id: Some(agent.profile_id),
                        executor_type: agent.executor_type,
                        availability: SoloAgentAvailability::Authenticated,
                        display_name: Some(agent.name),
                    });
                }
            }
        }
        self.evaluate_candidate_inputs(owner_id, inputs).await
    }

    async fn discovered_candidate_inputs(
        &self,
        owner_id: &str,
    ) -> Result<Vec<SoloAgentCandidateInput>> {
        Ok(list_agents(&self.db)
            .await?
            .into_iter()
            .filter(|agent| agent.owner_id.as_deref() == Some(owner_id))
            .map(|agent| SoloAgentCandidateInput {
                identity_id: agent.id,
                profile_id: Some(agent.profile_id),
                executor_type: agent.executor_type,
                // The source check below is authoritative.  This value is a
                // discovery hint, never an eligibility grant.
                availability: SoloAgentAvailability::Authenticated,
                display_name: Some(agent.name),
            })
            .collect())
    }

    async fn evaluate_candidate_inputs(
        &self,
        owner_id: &str,
        inputs: Vec<SoloAgentCandidateInput>,
    ) -> Result<Vec<CandidateEvaluation>> {
        let mut seen = HashSet::new();
        let mut evaluations = Vec::with_capacity(inputs.len());
        for input in inputs {
            let identity_id = input.identity_id.trim().to_owned();
            if identity_id.is_empty() {
                continue;
            }
            if !seen.insert(identity_id.clone()) {
                return Err(conflict(format!(
                    "duplicate Solo Agent candidate {identity_id}"
                )));
            }
            evaluations.push(self.evaluate_candidate(owner_id, input).await?);
        }
        evaluations.sort_by(|left, right| {
            left.candidate
                .executor_type
                .cmp(&right.candidate.executor_type)
                .then_with(|| {
                    left.candidate
                        .display_name
                        .cmp(&right.candidate.display_name)
                })
                .then_with(|| left.candidate.identity_id.cmp(&right.candidate.identity_id))
        });
        Ok(evaluations)
    }

    async fn evaluate_candidate(
        &self,
        owner_id: &str,
        input: SoloAgentCandidateInput,
    ) -> Result<CandidateEvaluation> {
        let identity_id = input.identity_id.trim().to_owned();
        let executor_type = input.executor_type.trim().to_ascii_lowercase();
        let display_name = input
            .display_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&executor_type)
            .to_owned();
        let mut candidate = SoloAgentCandidate {
            identity_id: identity_id.clone(),
            profile_id: input.profile_id.clone(),
            display_name,
            executor_type: executor_type.clone(),
            availability: input.availability,
            health: SoloAgentHealth::Unavailable,
            eligible: false,
            reason: None,
            next_step: None,
        };

        if !allowed_cli_executor(&executor_type) {
            candidate.reason = Some("unsupported_cli_executor".to_owned());
            candidate.next_step = Some("choose a supported authenticated CLI harness".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: None,
            });
        }
        if !input.availability.is_authenticated() {
            candidate.reason = Some(availability_reason(input.availability).to_owned());
            candidate.next_step = Some(availability_next_step(input.availability).to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: None,
            });
        }

        let Some(agent) = AgentRepo::get_by_id(&*self.db, &identity_id).await? else {
            candidate.availability = SoloAgentAvailability::Unknown;
            candidate.reason = Some("agent_identity_missing".to_owned());
            candidate.next_step = Some("refresh local Agent discovery".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: None,
            });
        };
        if agent.owner_id.as_deref() != Some(owner_id) {
            // Do not disclose another scope's identity in a picker.
            candidate.availability = SoloAgentAvailability::Unknown;
            candidate.reason = Some("agent_not_owned_by_solo_store".to_owned());
            candidate.next_step = Some("choose an Agent owned by this Solo store".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: None,
            });
        }
        if agent.executor_type != executor_type {
            candidate.reason = Some("executor_changed".to_owned());
            candidate.next_step = Some("refresh local Agent discovery".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: Some(agent),
            });
        }
        if let Some(profile_id) = input.profile_id.as_deref() {
            if profile_id != agent.profile_id {
                candidate.reason = Some("profile_changed".to_owned());
                candidate.next_step = Some("refresh the Agent profile".to_owned());
                return Ok(CandidateEvaluation {
                    candidate,
                    agent: Some(agent),
                });
            }
        }
        candidate.profile_id = Some(agent.profile_id.clone());
        candidate.display_name = agent.name.clone();

        let Some(profile) = AgentProfileRepo::get_profile(&*self.db, &agent.profile_id)
            .await?
            .filter(|profile| profile.identity_id == agent.id)
        else {
            candidate.reason = Some("agent_profile_missing".to_owned());
            candidate.next_step = Some("refresh local Agent discovery".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: Some(agent),
            });
        };
        if profile.backend_kind != "cli"
            || profile.executor_type != executor_type
            || agent.backend_kind != "cli"
        {
            candidate.reason = Some("not_a_local_cli_profile".to_owned());
            candidate.next_step = Some("choose a supported local CLI harness".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: Some(agent),
            });
        }

        if let Some(credential_ref) = profile.credential_ref.as_deref() {
            let configured = db::CredentialHandleRepo::get_credential_handle_for_owner(
                &*self.db,
                credential_ref,
                owner_id,
            )
            .await?
            .is_some_and(|handle| handle.status == "configured" && handle.enabled);
            if !configured {
                candidate.availability = SoloAgentAvailability::Disabled;
                candidate.reason = Some("credential_source_disabled".to_owned());
                candidate.next_step = Some("enable or authenticate the CLI credential".to_owned());
                return Ok(CandidateEvaluation {
                    candidate,
                    agent: Some(agent),
                });
            }
        }

        if !cli_runtime_source_enabled(&self.db, &agent).await? {
            candidate.availability = SoloAgentAvailability::Unauthenticated;
            candidate.reason = Some("cli_harness_not_authenticated".to_owned());
            candidate.next_step = Some("log in to the CLI and retry discovery".to_owned());
            return Ok(CandidateEvaluation {
                candidate,
                agent: Some(agent),
            });
        }

        match compute_effective_status(&self.db, &agent).await? {
            EffectiveStatus::Active => {
                candidate.health = SoloAgentHealth::Healthy;
                candidate.eligible = true;
            }
            EffectiveStatus::Busy => {
                // Capacity is enforced at Task claim.  Busy is still a valid
                // identity selection and is shown distinctly to the picker.
                candidate.health = SoloAgentHealth::Busy;
                candidate.eligible = true;
            }
            status => {
                candidate.availability = SoloAgentAvailability::Unknown;
                candidate.reason = Some(format!("agent_source_{}", status.as_str()));
                candidate.next_step = Some(status_next_step(&status).to_owned());
            }
        }
        Ok(CandidateEvaluation {
            candidate,
            agent: Some(agent),
        })
    }

    async fn require_selected_candidates(
        &self,
        metadata: &SoloProjectMetadata,
        evaluations: &[CandidateEvaluation],
        final_check: bool,
    ) -> Result<()> {
        for (label, selected_id) in [
            (
                "Project Agent",
                metadata.selected_project_agent_id.as_deref(),
            ),
            ("Task Worker", metadata.selected_worker_agent_id.as_deref()),
        ] {
            let Some(selected_id) = selected_id else {
                continue;
            };
            let evaluation = evaluations
                .iter()
                .find(|evaluation| evaluation.candidate.identity_id == selected_id)
                .ok_or_else(|| {
                    conflict(format!(
                        "selected {label} {selected_id} is not in current local Agent discovery"
                    ))
                })?;
            if !evaluation.candidate.eligible {
                return Err(conflict(format!(
                    "selected {label} {selected_id} is unavailable: {}",
                    evaluation
                        .candidate
                        .reason
                        .as_deref()
                        .unwrap_or("structured eligibility check failed")
                )));
            }
            if final_check && evaluation.agent.is_none() {
                return Err(conflict(format!(
                    "selected {label} {selected_id} disappeared during bootstrap"
                )));
            }
        }
        if let Some(project_agent_id) = metadata.selected_project_agent_id.as_deref() {
            let agent = evaluations
                .iter()
                .find(|evaluation| evaluation.candidate.identity_id == project_agent_id)
                .and_then(|evaluation| evaluation.agent.as_ref())
                .ok_or_else(|| conflict("selected Project Agent disappeared"))?;
            let profile = AgentProfileRepo::get_profile(&*self.db, &agent.profile_id)
                .await?
                .ok_or_else(|| conflict("selected Project Agent profile disappeared"))?;
            if !policy_allows_project_chat(&profile.tool_policy_json) {
                return Err(conflict(
                    "selected Project Agent identity is not permitted for Project Chat scope",
                ));
            }
        }
        if let Some(worker_id) = metadata.selected_worker_agent_id.as_deref() {
            let agent = evaluations
                .iter()
                .find(|evaluation| evaluation.candidate.identity_id == worker_id)
                .and_then(|evaluation| evaluation.agent.as_ref())
                .ok_or_else(|| conflict("selected Task Worker disappeared"))?;
            let profile = AgentProfileRepo::get_profile(&*self.db, &agent.profile_id)
                .await?
                .ok_or_else(|| conflict("selected Task Worker profile disappeared"))?;
            if !policy_allows_task_write(&profile.tool_policy_json) {
                return Err(conflict(
                    "selected Task Worker identity is not permitted for Task scope",
                ));
            }
        }
        Ok(())
    }

    async fn persist_selection_metadata(
        &self,
        mut project: Project,
        metadata: &SoloProjectMetadata,
    ) -> Result<Project> {
        let current_metadata = parse_project_metadata(&project)?;
        if current_metadata == *metadata {
            return Ok(project);
        }
        if !same_immutable_metadata(&current_metadata, metadata) {
            return Err(conflict(
                "Solo Project selection metadata conflicts with the durable setup",
            ));
        }
        let settings = update_project_settings(
            &project.settings,
            metadata,
            metadata.selected_worker_agent_id.as_deref(),
        )?;
        let expected_version = project.version;
        project = ProjectRepo::update_at_version(
            &*self.db,
            UpdateProject {
                id: project.id.clone(),
                name: None,
                settings: Some(settings),
                primary_repo_id: None,
                paused_at: None,
                updated_at: db::now_rfc3339(),
            },
            expected_version,
            None,
        )
        .await
        .map_err(|error| match error {
            db::DbError::VersionConflict => ServiceError::Conflict(
                "Solo Project changed while confirming Agent selection".to_owned(),
            ),
            other => ServiceError::Db(other),
        })?;
        Ok(project)
    }

    async fn ensure_owner_membership(
        &self,
        project: &Project,
        owner_id: &str,
    ) -> Result<ProjectMember> {
        let members = ProjectMemberRepo::list_members(&*self.db, &project.id).await?;
        if members.len() > 1 {
            return Err(conflict(
                "Solo Project contains more than its one internal owner membership",
            ));
        }
        if let Some(member) = members.into_iter().next() {
            if member.user_id != owner_id || member.role != "owner" {
                return Err(conflict(
                    "Solo Project owner membership does not match the internal owner",
                ));
            }
            return Ok(member);
        }
        let now = db::now_rfc3339();
        match ProjectMemberRepo::add_member(
            &*self.db,
            CreateProjectMember {
                id: db::new_uuid_v4(),
                project_id: project.id.clone(),
                user_id: owner_id.to_owned(),
                role: "owner".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        {
            Ok(member) => Ok(member),
            Err(error) => {
                // A concurrent/replayed insert is reconciled by the typed
                // read.  Other errors are not hidden as partial success.
                if let Some(member) =
                    ProjectMemberRepo::get_member(&*self.db, &project.id, owner_id).await?
                {
                    if member.role == "owner" {
                        return Ok(member);
                    }
                }
                Err(error.into())
            }
        }
    }

    async fn ensure_local_repo(
        &self,
        project: &mut Project,
        request: &SoloBootstrapRequest,
    ) -> Result<Repo> {
        let repos = list_project_repos(&self.db, &project.id).await?;
        let primary = match project.primary_repo_id.as_deref() {
            Some(primary_id) => Some(
                repos
                    .iter()
                    .find(|repo| repo.id == primary_id)
                    .cloned()
                    .ok_or_else(|| conflict("Solo Project primary Repo is missing"))?,
            ),
            None => None,
        };
        if let Some(primary) = primary.as_ref() {
            verify_local_repo_identity(primary, request)?;
            if repos
                .iter()
                .any(|repo| repo.id != primary.id && is_same_source_path(repo, request))
            {
                return Err(conflict(
                    "Solo Project has more than one Repo for the local source checkout",
                ));
            }
            return self.reconcile_repo_source_path(primary, request).await;
        }

        let matching = repos
            .iter()
            .filter(|repo| is_same_repo_identity(repo, request))
            .cloned()
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return Err(conflict(
                "Solo Project has ambiguous Repo bindings for this repository identity",
            ));
        }
        let repo = if let Some(repo) = matching.into_iter().next() {
            if repos
                .iter()
                .any(|other| other.id != repo.id && is_same_source_path(other, request))
            {
                return Err(conflict(
                    "Solo Project has more than one Repo for the local source checkout",
                ));
            }
            self.reconcile_repo_source_path(&repo, request).await?
        } else {
            if !repos.is_empty() {
                return Err(conflict(
                    "Solo Project contains a Repo that does not match this repository identity",
                ));
            }
            let now = db::now_rfc3339();
            RepoRepo::create(
                &*self.db,
                CreateRepo {
                    id: db::new_uuid_v4(),
                    project_id: project.id.clone(),
                    name: repository_name(request),
                    // The Repo schema requires a remote URL even for a local
                    // source.  The canonical identity is stable and avoids a
                    // fabricated network origin.
                    remote_url: request.canonical_repository.clone(),
                    local_path: Some(request.source_path().to_owned()),
                    work_mode: WorkMode::DirectMerge,
                    default_branch: request.default_branch.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await?
        };
        if project.primary_repo_id.is_none() {
            let expected_version = project.version;
            match ProjectRepo::update_at_version(
                &*self.db,
                UpdateProject {
                    id: project.id.clone(),
                    name: None,
                    settings: None,
                    primary_repo_id: Some(Some(repo.id.clone())),
                    paused_at: None,
                    updated_at: db::now_rfc3339(),
                },
                expected_version,
                None,
            )
            .await
            {
                Ok(updated) => *project = updated,
                Err(db::DbError::VersionConflict) => {
                    let current = ProjectRepo::get_by_id(&*self.db, &project.id)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("project", project.id.clone()))?;
                    if current.primary_repo_id.as_deref() != Some(repo.id.as_str()) {
                        return Err(conflict(
                            "Solo Project primary Repo changed during bootstrap",
                        ));
                    }
                    *project = current;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(repo)
    }

    async fn reconcile_repo_source_path(
        &self,
        repo: &Repo,
        request: &SoloBootstrapRequest,
    ) -> Result<Repo> {
        if repo.local_path.as_deref() == Some(request.source_path()) {
            return Ok(repo.clone());
        }
        RepoRepo::update(
            &*self.db,
            UpdateRepo {
                id: repo.id.clone(),
                name: None,
                local_path: Some(Some(request.source_path().to_owned())),
                remote_url: None,
                work_mode: None,
                default_branch: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn ensure_chat(&self, project: &Project) -> Result<AgentChat> {
        let chat = AgentChatRepo::get_project_chat(&*self.db, &project.id)
            .await?
            .ok_or_else(|| conflict("Solo Project is missing its canonical Project Chat"))?;
        if chat.kind != "project" || chat.project_id.as_deref() != Some(project.id.as_str()) {
            return Err(conflict("Solo Project Chat is not bound to the Project"));
        }
        Ok(chat)
    }

    async fn ensure_binding(
        &self,
        project: &Project,
        chat: &AgentChat,
        metadata: &SoloProjectMetadata,
    ) -> Result<ProjectAgentBinding> {
        let binding = ProjectAgentBindingRepo::get_active_project_binding(&*self.db, &project.id)
            .await?
            .ok_or_else(|| conflict("Solo Project is missing its canonical Agent binding"))?;
        match project.charter_status.as_str() {
            "legacy_unverified" if project.charter_setup_required => match binding.state.as_str() {
                PROJECT_AGENT_BINDING_SETUP_STATE => {
                    if binding.identity_id.is_some()
                        || binding.profile_id.is_some()
                        || !binding.charter_setup_required
                        || chat.status != PROJECT_CHAT_SETUP_STATUS
                    {
                        return Err(conflict(
                            "Solo Project setup binding/chat is not in the legacy adoption state",
                        ));
                    }
                }
                "active" => {
                    let Some(selected_id) = metadata.selected_project_agent_id.as_deref() else {
                        return Err(conflict(
                            "active legacy Project Agent binding has no confirmed Solo selection",
                        ));
                    };
                    let Some(identity_id) = binding.identity_id.as_deref() else {
                        return Err(conflict(
                            "active legacy Project Agent binding has no identity",
                        ));
                    };
                    let Some(profile_id) = binding.profile_id.as_deref() else {
                        return Err(conflict(
                            "active legacy Project Agent binding has no profile",
                        ));
                    };
                    let agent = AgentRepo::get_by_id(&*self.db, identity_id)
                        .await?
                        .ok_or_else(|| {
                            conflict("active legacy Project Agent binding identity is missing")
                        })?;
                    if identity_id != selected_id
                        || agent.profile_id != profile_id
                        || !binding.charter_setup_required
                        || binding.admission_receipt_id.is_some()
                        || binding.charter_approval_id.is_some()
                        || binding.charter_id.is_some()
                        || binding.charter_revision_id.is_some()
                        || chat.status != PROJECT_CHAT_READY_STATUS
                        || binding.permission_ceiling_json != SOLO_PROJECT_PERMISSION_CEILING_JSON
                    {
                        return Err(conflict(
                                "active legacy Project Agent binding/chat is not in the adoption-only state",
                            ));
                    }
                    let current_skill =
                        ProjectAdmissionReceiptRepo::get_current_project_operating_skill_revision(
                            &*self.db,
                        )
                        .await?;
                    if binding.operating_skill_revision_id.as_deref() != current_skill.as_deref() {
                        return Err(conflict(
                                "active legacy Project Agent binding does not use the current Project operating skill",
                            ));
                    }
                }
                _ => {
                    return Err(conflict(
                        "Solo Project setup binding/chat is not in the legacy adoption state",
                    ));
                }
            },
            "charter_backed" if !project.charter_setup_required => {
                if binding.state != "active"
                    || binding.identity_id.is_none()
                    || binding.profile_id.is_none()
                    || binding.charter_setup_required
                    || chat.status != PROJECT_CHAT_READY_STATUS
                {
                    return Err(conflict(
                        "Solo Project Charter-backed binding/chat is inconsistent",
                    ));
                }
                if ProjectAdmissionReceiptRepo::resolve_current_project_binding_authority(
                    &*self.db,
                    &project.id,
                )
                .await?
                .is_none()
                {
                    return Err(conflict(
                        "Solo Project Charter-backed binding has no admission authority",
                    ));
                }
            }
            _ => {
                return Err(conflict(
                    "Solo Project Charter state is not a supported adoption or ready state",
                ));
            }
        }
        Ok(binding)
    }

    async fn activate_project_agent_for_adoption(
        &self,
        project: &Project,
        metadata: &SoloProjectMetadata,
        binding: &ProjectAgentBinding,
    ) -> Result<ProjectAgentBinding> {
        let identity_id = metadata
            .selected_project_agent_id
            .as_deref()
            .ok_or_else(|| conflict("Project Agent adoption requires a confirmed identity"))?;
        if binding.state != PROJECT_AGENT_BINDING_SETUP_STATE {
            return Ok(binding.clone());
        }
        let agent_chat_service = crate::AgentChatService::new(Arc::clone(&self.db));
        agent_chat_service
            .set_project_binding(crate::SetProjectAgentBindingInput {
                actor_user_id: metadata.owner_id.clone(),
                project_id: project.id.clone(),
                identity_id: Some(identity_id.to_owned()),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: SOLO_PROJECT_PERMISSION_CEILING_JSON.to_owned(),
                subscriptions_json: "[]".to_owned(),
                // This is the existing typed binding field; a one-turn
                // adoption budget is sufficient and does not become Task
                // execution authority.
                wake_budget: 1,
                expected_version: Some(binding.version),
                replacement_reason: Some(
                    "Forge Solo confirmed Project Agent; Charter adoption pending".to_owned(),
                ),
            })
            .await
    }

    async fn result(
        &self,
        request: &SoloBootstrapRequest,
        receipt: (&str, &str),
        metadata: &SoloProjectMetadata,
        records: (&Project, &Repo, &AgentChat, &ProjectAgentBinding),
        evaluations: Vec<CandidateEvaluation>,
    ) -> Result<SoloBootstrapResult> {
        let (idempotency_key, input_digest) = receipt;
        let (project, repo, chat, binding) = records;
        let candidates = evaluations
            .iter()
            .map(|evaluation| evaluation.candidate.clone())
            .collect::<Vec<_>>();
        let project_agent_identity_id = binding.identity_id.clone();
        let project_agent_profile_id = binding.profile_id.clone();
        let selected_project_agent_id = metadata.selected_project_agent_id.clone();
        if let Some(identity_id) = project_agent_identity_id.as_deref() {
            if metadata
                .selected_project_agent_id
                .as_deref()
                .is_some_and(|selected| selected != identity_id)
            {
                return Err(conflict(
                    "active Project Agent binding does not match Solo metadata",
                ));
            }
        }
        let worker_identity_id = metadata.selected_worker_agent_id.clone();
        let project_agent_selected = selected_project_agent_id
            .as_deref()
            .or(project_agent_identity_id.as_deref());
        let readiness = if project_agent_selected.is_none() {
            SoloBootstrapReadiness::AgentSelectionRequired
        } else if worker_identity_id.is_none() {
            SoloBootstrapReadiness::WorkerSelectionRequired
        } else if project.charter_status == "legacy_unverified" && project.charter_setup_required {
            SoloBootstrapReadiness::CharterAdoptionRequired
        } else {
            SoloBootstrapReadiness::Ready
        };
        let suggested_project_agent_id = if project_agent_identity_id.is_none() {
            let eligible = candidates.iter().filter(|candidate| candidate.eligible);
            let mut ids = eligible.map(|candidate| candidate.identity_id.as_str());
            let first = ids.next().map(str::to_owned);
            if ids.next().is_none() {
                first
            } else {
                None
            }
        } else {
            None
        };
        Ok(SoloBootstrapResult {
            idempotency_key: idempotency_key.to_owned(),
            input_digest: input_digest.to_owned(),
            repository_id: request.repository_id.clone(),
            canonical_repository: request.canonical_repository.clone(),
            data_root: request.data_root.clone(),
            owner_id: metadata.owner_id.clone(),
            project_id: project.id.clone(),
            repo_id: repo.id.clone(),
            project_chat_id: chat.id.clone(),
            project_agent_binding_id: binding.id.clone(),
            project_agent_identity_id,
            project_agent_profile_id,
            selected_project_agent_id,
            worker_identity_id,
            selected_worker_agent_id: metadata.selected_worker_agent_id.clone(),
            workflow_template_name: SOLO_WORKFLOW_TEMPLATE_NAME.to_owned(),
            readiness,
            agent_candidates: candidates,
            suggested_project_agent_id,
            adoption_required: project.charter_status == "legacy_unverified"
                && project.charter_setup_required,
            mutation_authority_granted: project.charter_status == "charter_backed"
                && !project.charter_setup_required
                && binding.state == "active"
                && !binding.charter_setup_required,
        })
    }
}

fn normalize_request(mut request: SoloBootstrapRequest) -> Result<SoloBootstrapRequest> {
    request.repository_id = request.repository_id.trim().to_owned();
    request.canonical_repository = request.canonical_repository.trim().to_owned();
    request.data_root = request.data_root.trim().to_owned();
    request.default_branch = request.default_branch.trim().to_owned();
    if !validate_uuid_v4(&request.repository_id) {
        return Err(ServiceError::invalid_operation(
            "repository_id must be a valid UUID v4",
        ));
    }
    required("canonical_repository", &request.canonical_repository)?;
    required("data_root", &request.data_root)?;
    required("default_branch", &request.default_branch)?;
    if request
        .source_path
        .as_deref()
        .is_some_and(|path| path.trim().is_empty())
    {
        return Err(ServiceError::invalid_operation(
            "source_path must not be empty",
        ));
    }
    if let Some(source_path) = request.source_path.as_mut() {
        *source_path = source_path.trim().to_owned();
    }
    if let Some(name) = request.repository_name.as_mut() {
        *name = name.trim().to_owned();
    }
    for selected in [
        request.selected_project_agent_id.as_mut(),
        request.selected_worker_agent_id.as_mut(),
    ]
    .into_iter()
    .flatten()
    {
        *selected = selected.trim().to_owned();
        if selected.is_empty() {
            return Err(ServiceError::invalid_operation(
                "selected Agent identity IDs must not be empty",
            ));
        }
    }
    Ok(request)
}

fn bootstrap_identity(request: &SoloBootstrapRequest) -> Result<(String, String)> {
    let digest = canonical_digest_with_schema(
        SOLO_BOOTSTRAP_CONTRACT_REVISION,
        &SoloBootstrapDigestInput {
            repository_id: &request.repository_id,
            canonical_repository: &request.canonical_repository,
            data_root: &request.data_root,
            default_branch: &request.default_branch,
        },
    )
    .map_err(|error| {
        ServiceError::invalid_operation(format!("cannot digest Solo bootstrap input: {error}"))
    })?;
    Ok((
        format!(
            "solo-bootstrap:{}:{}",
            request.repository_id, SOLO_BOOTSTRAP_CONTRACT_REVISION
        ),
        digest,
    ))
}

fn inspect_project_scope(
    projects: &[Project],
    request: &SoloBootstrapRequest,
    idempotency_key: &str,
    input_digest: &str,
) -> Result<Option<SoloProjectMetadata>> {
    if projects.is_empty() {
        return Ok(None);
    }
    if projects.len() > 1 {
        return Err(conflict(
            "Solo data root contains more than one Project; choose an explicit recovery directory",
        ));
    }
    let metadata = parse_project_metadata(&projects[0])?;
    if metadata.contract_revision != SOLO_BOOTSTRAP_CONTRACT_REVISION
        || metadata.repository_id != request.repository_id
        || metadata.canonical_repository != request.canonical_repository
        || metadata.data_root != request.data_root
        || metadata.default_branch != request.default_branch
        || metadata.idempotency_key != idempotency_key
        || metadata.input_digest != input_digest
        || metadata.workflow_template_name != SOLO_WORKFLOW_TEMPLATE_NAME
        || metadata.owner_id.trim().is_empty()
    {
        return Err(conflict(
            "Solo data root is bound to a different repository or bootstrap contract",
        ));
    }
    Ok(Some(metadata))
}

fn parse_project_metadata(project: &Project) -> Result<SoloProjectMetadata> {
    let value = serde_json::from_str::<Value>(&project.settings)
        .map_err(|error| conflict(format!("Solo Project settings are malformed: {error}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| conflict("Solo Project settings must be a JSON object".to_owned()))?;
    let metadata = object.get(SOLO_PROJECT_SETTINGS_KEY).ok_or_else(|| {
        conflict("Solo data root contains a Project without Solo binding metadata".to_owned())
    })?;
    let metadata =
        serde_json::from_value::<SoloProjectMetadata>(metadata.clone()).map_err(|error| {
            conflict(format!(
                "Solo Project binding metadata is malformed: {error}"
            ))
        })?;
    validate_solo_settings(object, &metadata)?;
    Ok(metadata)
}

fn validate_solo_settings(
    object: &Map<String, Value>,
    metadata: &SoloProjectMetadata,
) -> Result<()> {
    if object.get("workflow_template_name").and_then(Value::as_str)
        != Some(SOLO_WORKFLOW_TEMPLATE_NAME)
    {
        return Err(conflict(
            "Solo Project workflow template marker is not autonomous_v1",
        ));
    }
    let assignments = object
        .get("default_role_assignments")
        .and_then(Value::as_array)
        .ok_or_else(|| conflict("default_role_assignments must be an array"))?;
    let worker_assignments = assignments
        .iter()
        .filter(|assignment| {
            assignment.get("role_name").and_then(Value::as_str) == Some(default_roles::WORKER)
        })
        .collect::<Vec<_>>();
    if worker_assignments.len() > 1 {
        return Err(conflict(
            "Solo Project has more than one default Task Worker assignment",
        ));
    }
    match (
        metadata.selected_worker_agent_id.as_deref(),
        worker_assignments.first(),
    ) {
        (Some(expected_id), Some(assignment))
            if assignment.get("assignee_type").and_then(Value::as_str) == Some("agent")
                && assignment.get("assignee_id").and_then(Value::as_str) == Some(expected_id) => {}
        (Some(_), Some(_)) => {
            return Err(conflict(
                "default Task Worker assignment conflicts with Solo metadata",
            ));
        }
        (Some(_), None) => {
            return Err(conflict(
                "Solo metadata names a Task Worker without a default assignment",
            ));
        }
        (None, Some(_)) => {
            return Err(conflict(
                "default Task Worker assignment exists without Solo metadata",
            ));
        }
        (None, None) => {}
    }
    Ok(())
}

fn project_settings(metadata: &SoloProjectMetadata, worker_id: Option<&str>) -> Result<String> {
    let mut object = Map::new();
    object.insert(
        SOLO_PROJECT_SETTINGS_KEY.to_owned(),
        serde_json::to_value(metadata).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "Solo Project metadata is not serializable: {error}"
            ))
        })?,
    );
    object.insert(
        "workflow_template_name".to_owned(),
        Value::String(SOLO_WORKFLOW_TEMPLATE_NAME.to_owned()),
    );
    let assignments = worker_id
        .map(|identity_id| {
            json!([{
                "role_name": default_roles::WORKER,
                "assignee_type": "agent",
                "assignee_id": identity_id,
            }])
        })
        .unwrap_or_else(|| json!([]));
    object.insert("default_role_assignments".to_owned(), assignments);
    serde_json::to_string(&Value::Object(object)).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Solo Project settings are not serializable: {error}"
        ))
    })
}

fn same_immutable_metadata(left: &SoloProjectMetadata, right: &SoloProjectMetadata) -> bool {
    left.contract_revision == right.contract_revision
        && left.repository_id == right.repository_id
        && left.canonical_repository == right.canonical_repository
        && left.data_root == right.data_root
        && left.default_branch == right.default_branch
        && left.idempotency_key == right.idempotency_key
        && left.input_digest == right.input_digest
        && left.owner_id == right.owner_id
        && left.workflow_template_name == right.workflow_template_name
}

/// Merge a newly confirmed setup selection into the existing Project settings.
/// The settings object may contain unrelated user-controlled keys, so only
/// the Solo metadata, workflow marker, and worker assignment are reconciled.
/// A conflicting worker assignment is never silently replaced.
fn update_project_settings(
    settings_json: &str,
    metadata: &SoloProjectMetadata,
    worker_id: Option<&str>,
) -> Result<String> {
    let mut settings = serde_json::from_str::<Value>(settings_json)
        .map_err(|error| conflict(format!("Solo Project settings are malformed: {error}")))?;
    let object = settings
        .as_object_mut()
        .ok_or_else(|| conflict("Solo Project settings must be a JSON object"))?;

    let current_metadata = object
        .get(SOLO_PROJECT_SETTINGS_KEY)
        .ok_or_else(|| conflict("Solo Project is missing its Solo binding metadata".to_owned()))?;
    let current_metadata = serde_json::from_value::<SoloProjectMetadata>(current_metadata.clone())
        .map_err(|error| {
            conflict(format!(
                "Solo Project binding metadata is malformed: {error}"
            ))
        })?;
    if !same_immutable_metadata(&current_metadata, metadata) {
        return Err(conflict(
            "Solo Project selection update conflicts with immutable metadata",
        ));
    }
    object.insert(
        SOLO_PROJECT_SETTINGS_KEY.to_owned(),
        serde_json::to_value(metadata).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "Solo Project metadata is not serializable: {error}"
            ))
        })?,
    );

    if let Some(existing_template) = object
        .get("workflow_template_name")
        .and_then(Value::as_str)
        .filter(|name| *name != SOLO_WORKFLOW_TEMPLATE_NAME)
    {
        return Err(conflict(format!(
            "Solo Project workflow template is not autonomous_v1: {existing_template}"
        )));
    }
    object.insert(
        "workflow_template_name".to_owned(),
        Value::String(SOLO_WORKFLOW_TEMPLATE_NAME.to_owned()),
    );

    let assignments = object
        .entry("default_role_assignments".to_owned())
        .or_insert_with(|| Value::Array(Vec::new()));
    let assignments = assignments
        .as_array_mut()
        .ok_or_else(|| conflict("default_role_assignments must be an array"))?;
    let worker_positions = assignments
        .iter()
        .enumerate()
        .filter(|(_, assignment)| {
            assignment.get("role_name").and_then(Value::as_str) == Some(default_roles::WORKER)
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if worker_positions.len() > 1 {
        return Err(conflict(
            "Solo Project has more than one default Task Worker assignment",
        ));
    }

    match (worker_id, worker_positions.first().copied()) {
        (Some(worker_id), Some(index)) => {
            let assignment = assignments
                .get(index)
                .and_then(Value::as_object)
                .ok_or_else(|| conflict("default Task Worker assignment must be an object"))?;
            let existing_type = assignment
                .get("assignee_type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let existing_id = assignment
                .get("assignee_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if existing_type != "agent" || existing_id != worker_id {
                return Err(conflict(
                    "default Task Worker assignment conflicts with Solo selection",
                ));
            }
        }
        (Some(worker_id), None) => assignments.push(json!({
            "role_name": default_roles::WORKER,
            "assignee_type": "agent",
            "assignee_id": worker_id,
        })),
        (None, Some(_)) => {
            return Err(conflict(
                "default Task Worker assignment exists without a Solo Worker selection",
            ));
        }
        (None, None) => {}
    }

    serde_json::to_string(&settings).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Solo Project settings are not serializable: {error}"
        ))
    })
}

fn verify_project_workflow(
    project: &Project,
    expected: &api_types::WorkflowDefinition,
) -> Result<()> {
    let actual =
        serde_json::from_str::<api_types::WorkflowDefinition>(&project.workflow_definition)
            .map_err(|error| conflict(format!("Solo Project workflow is malformed: {error}")))?;
    if actual != *expected {
        return Err(conflict(
            "Solo Project workflow is not the autonomous_v1 definition",
        ));
    }
    if project
        .workflow_template_name
        .as_deref()
        .is_some_and(|name| name != SOLO_WORKFLOW_TEMPLATE_NAME)
    {
        return Err(conflict(
            "Solo Project workflow template is not autonomous_v1",
        ));
    }
    Ok(())
}

fn reconcile_requested_selection(
    metadata: &mut SoloProjectMetadata,
    request: &SoloBootstrapRequest,
    evaluations: &[CandidateEvaluation],
    owner_id: &str,
) -> Result<()> {
    if metadata.owner_id != owner_id {
        return Err(conflict("Solo Project metadata owner mismatch"));
    }
    for (label, durable, requested) in [
        (
            "Project Agent",
            &mut metadata.selected_project_agent_id,
            request.selected_project_agent_id.as_deref(),
        ),
        (
            "Task Worker",
            &mut metadata.selected_worker_agent_id,
            request.selected_worker_agent_id.as_deref(),
        ),
    ] {
        if let Some(requested) = requested {
            if let Some(existing) = durable.as_deref() {
                if existing != requested {
                    return Err(conflict(format!(
                        "requested {label} differs from the durable Solo selection"
                    )));
                }
            } else {
                *durable = Some(requested.to_owned());
            }
        }
    }
    if metadata.selected_project_agent_id.is_none() && metadata.selected_worker_agent_id.is_some() {
        return Err(ServiceError::invalid_operation(
            "Task Worker selection requires a confirmed Project Agent",
        ));
    }
    // Project Agent and Task Worker are independent selections. Reusing an
    // identity is permitted only after the Worker policy check below; a
    // separate supported harness may use a different executor type.
    if let Some(project_agent) = metadata.selected_project_agent_id.as_deref() {
        evaluations
            .iter()
            .find(|evaluation| evaluation.candidate.identity_id == project_agent)
            .ok_or_else(|| {
                conflict("selected Project Agent is not a discovered local candidate")
            })?;
    }
    Ok(())
}

fn owner_setting_key(repository_id: &str) -> String {
    format!("{SOLO_OWNER_SETTING_PREFIX}{repository_id}")
}

fn owner_email(repository_id: &str) -> String {
    format!("forge-solo-owner+{repository_id}@invalid.local")
}

fn verify_owner_binding(binding: &SoloOwnerBinding, request: &SoloBootstrapRequest) -> Result<()> {
    if binding.contract_revision != SOLO_BOOTSTRAP_CONTRACT_REVISION
        || binding.repository_id != request.repository_id
        || binding.canonical_repository != request.canonical_repository
        || binding.data_root != request.data_root
        || binding.owner_id.trim().is_empty()
    {
        return Err(conflict(
            "Solo owner setting is bound to a different repository or data root",
        ));
    }
    Ok(())
}

fn verify_internal_owner(user: &User, repository_id: &str) -> Result<()> {
    if user.email != owner_email(repository_id) || user.is_admin {
        return Err(conflict(
            "Solo owner principal does not match the internal repository-scoped owner",
        ));
    }
    Ok(())
}

fn project_name(request: &SoloBootstrapRequest) -> String {
    let value = request
        .repository_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .map(str::trim)
        .map(str::to_owned)
        .or_else(|| {
            Path::new(request.source_path())
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.trim().is_empty())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "Solo Project".to_owned());
    value.chars().take(SOLO_PROJECT_NAME_MAX_CHARS).collect()
}

fn repository_name(request: &SoloBootstrapRequest) -> String {
    request
        .repository_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .map(str::trim)
        .map(str::to_owned)
        .or_else(|| {
            Path::new(request.source_path())
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.trim().is_empty())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "local".to_owned())
}

fn allowed_cli_executor(executor_type: &str) -> bool {
    matches!(
        executor_type.parse::<ExecutorKind>(),
        Ok(ExecutorKind::Codex)
            | Ok(ExecutorKind::ClaudeCode)
            | Ok(ExecutorKind::Cursor)
            | Ok(ExecutorKind::Opencode)
            | Ok(ExecutorKind::Gemini)
            | Ok(ExecutorKind::Smith)
    )
}

fn policy_allows_task_write(tool_policy_json: &str) -> bool {
    policy_has_permission(tool_policy_json, "task_write")
}

fn policy_allows_project_chat(tool_policy_json: &str) -> bool {
    policy_has_permission(tool_policy_json, "read_agent_chat")
        && policy_has_permission(tool_policy_json, "propose_message")
}

fn policy_has_permission(tool_policy_json: &str, expected: &str) -> bool {
    serde_json::from_str::<Value>(tool_policy_json)
        .ok()
        .and_then(|value| match value {
            Value::Array(values) => Some(values),
            Value::Object(map) => map
                .get("permissions")
                .or_else(|| map.get("allowed"))
                .and_then(Value::as_array)
                .cloned(),
            _ => None,
        })
        .is_some_and(|permissions| {
            permissions
                .iter()
                .any(|permission| permission.as_str() == Some(expected))
        })
}

fn availability_reason(availability: SoloAgentAvailability) -> &'static str {
    match availability {
        SoloAgentAvailability::Authenticated => "authenticated",
        SoloAgentAvailability::Installed => "cli_not_authenticated",
        SoloAgentAvailability::NotFound => "cli_not_found",
        SoloAgentAvailability::Unauthenticated => "cli_not_authenticated",
        SoloAgentAvailability::Disabled => "cli_source_disabled",
        SoloAgentAvailability::Unknown => "cli_availability_unknown",
    }
}

fn availability_next_step(availability: SoloAgentAvailability) -> &'static str {
    match availability {
        SoloAgentAvailability::Authenticated => "",
        SoloAgentAvailability::Installed | SoloAgentAvailability::Unauthenticated => {
            "log in to the CLI and retry discovery"
        }
        SoloAgentAvailability::NotFound => "install the supported CLI and retry discovery",
        SoloAgentAvailability::Disabled => "enable the local CLI source and retry discovery",
        SoloAgentAvailability::Unknown => "refresh local Agent discovery",
    }
}

fn status_next_step(status: &EffectiveStatus) -> &'static str {
    match status {
        EffectiveStatus::DaemonUnavailable | EffectiveStatus::DaemonOffline => {
            "start the local Forge daemon and retry discovery"
        }
        EffectiveStatus::SourceDisabled | EffectiveStatus::ConnectionUnavailable => {
            "authenticate or enable the local CLI source and retry discovery"
        }
        EffectiveStatus::ConnectionDegraded => "repair the local CLI connection and retry",
        EffectiveStatus::Paused => "unpause the Agent and retry discovery",
        EffectiveStatus::Error => "repair the Agent error and retry discovery",
        EffectiveStatus::Deactivated => "refresh the local CLI harness configuration",
        EffectiveStatus::Active | EffectiveStatus::Busy => "",
    }
}

fn is_same_repo_identity(repo: &Repo, request: &SoloBootstrapRequest) -> bool {
    repo.remote_url == request.canonical_repository
        && repo.default_branch == request.default_branch
        && repo.work_mode == WorkMode::DirectMerge
}

fn is_same_source_path(repo: &Repo, request: &SoloBootstrapRequest) -> bool {
    repo.local_path.as_deref() == Some(request.source_path())
}

fn verify_local_repo_identity(repo: &Repo, request: &SoloBootstrapRequest) -> Result<()> {
    if !is_same_repo_identity(repo, request) {
        return Err(conflict(
            "Solo Project primary Repo does not match the selected local repository",
        ));
    }
    Ok(())
}

fn required<'a>(field: &str, value: &'a str) -> Result<&'a str> {
    if value.trim().is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "{field} must not be empty"
        )));
    }
    Ok(value)
}

fn conflict(message: impl Into<String>) -> ServiceError {
    ServiceError::Conflict(message.into())
}

fn agent_registration_key(agent: &Agent) -> Option<String> {
    serde_json::from_str::<Value>(&agent.config_json)
        .ok()
        .and_then(|value| {
            value
                .get(SOLO_AGENT_REGISTRATION_KEY)
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

async fn list_agents(db: &SqliteDb) -> Result<Vec<Agent>> {
    list_pages(
        |page| {
            AgentRepo::list(
                db,
                AgentListQuery {
                    status: None,
                    executor_type: None,
                    capabilities: Vec::new(),
                    page,
                },
            )
        },
        "Solo Agent list exceeded the reconciliation bound",
    )
    .await
}

async fn list_projects(db: &SqliteDb) -> Result<Vec<Project>> {
    list_pages(
        |page| ProjectRepo::list(db, page),
        "Solo Project list exceeded the reconciliation bound",
    )
    .await
}

async fn list_project_repos(db: &SqliteDb, project_id: &str) -> Result<Vec<Repo>> {
    list_pages(
        |page| RepoRepo::list_by_project(db, project_id, page),
        "Solo Repo list exceeded the reconciliation bound",
    )
    .await
}

async fn list_pages<T, F, Fut>(mut fetch: F, bound_message: &str) -> Result<Vec<T>>
where
    F: FnMut(PageRequest) -> Fut,
    Fut: std::future::Future<Output = db::Result<Page<T>>>,
{
    let mut cursor = None;
    let mut pages = 0;
    let mut items = Vec::new();
    loop {
        pages += 1;
        if pages > MAX_RECONCILIATION_PAGES {
            return Err(conflict(bound_message));
        }
        let page = fetch(PageRequest {
            cursor,
            limit: 500,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        })
        .await?;
        items.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, run_migrations, AgentRepo, CreateAgent, DaemonStatus,
        UpdateDaemonReport, UpsertDaemon,
    };
    use tempfile::tempdir;

    async fn fixture() -> (Arc<SqliteDb>, SoloBootstrapService) {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        let db = Arc::new(SqliteDb::new(pool));
        let service = SoloBootstrapService::new(Arc::clone(&db));
        (db, service)
    }

    fn request(path: &Path, repo_id: &str) -> SoloBootstrapRequest {
        let mut request = SoloBootstrapRequest::new(
            repo_id,
            path.to_string_lossy().to_string(),
            path.join(".forge-solo-data").to_string_lossy().to_string(),
            "main",
        );
        request.repository_name = Some("demo".to_owned());
        request
    }

    #[tokio::test]
    async fn bootstrap_is_idempotent_and_keeps_charter_gate() {
        let (_db, service) = fixture().await;
        let source = tempdir().expect("temp repository");
        let repo_id = db::new_uuid_v4();
        let first = service
            .bootstrap(request(source.path(), &repo_id))
            .await
            .expect("first bootstrap");
        let second = service
            .bootstrap(request(source.path(), &repo_id))
            .await
            .expect("replay bootstrap");

        assert_eq!(first.owner_id, second.owner_id);
        assert_eq!(first.project_id, second.project_id);
        assert_eq!(first.repo_id, second.repo_id);
        assert_eq!(first.project_chat_id, second.project_chat_id);
        assert_eq!(
            first.project_agent_binding_id,
            second.project_agent_binding_id
        );
        assert_eq!(
            first.readiness,
            SoloBootstrapReadiness::AgentSelectionRequired
        );
        assert!(first.adoption_required);
        assert!(!first.mutation_authority_granted);

        let projects = list_projects(&_db).await.expect("projects");
        assert_eq!(projects.len(), 1);
        let repos = list_project_repos(&_db, &first.project_id)
            .await
            .expect("repos");
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].id, first.repo_id);
        assert_eq!(
            repos[0].local_path.as_deref(),
            Some(source.path().to_string_lossy().as_ref())
        );
        let members = ProjectMemberRepo::list_members(_db.as_ref(), &first.project_id)
            .await
            .expect("members");
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].user_id, first.owner_id);
        assert_eq!(members[0].role, "owner");
        let chat = AgentChatRepo::get_project_chat(_db.as_ref(), &first.project_id)
            .await
            .expect("chat")
            .expect("canonical chat");
        assert_eq!(chat.id, first.project_chat_id);
        let binding =
            ProjectAgentBindingRepo::get_active_project_binding(_db.as_ref(), &first.project_id)
                .await
                .expect("binding")
                .expect("setup binding");
        assert_eq!(binding.state, PROJECT_AGENT_BINDING_SETUP_STATE);
        assert!(binding.identity_id.is_none());
        assert!(binding.profile_id.is_none());
    }

    #[tokio::test]
    async fn a_mismatched_repository_fails_closed_without_a_second_project() {
        let (db, service) = fixture().await;
        let source = tempdir().expect("temp repository");
        let repo_id = db::new_uuid_v4();
        service
            .bootstrap(request(source.path(), &repo_id))
            .await
            .expect("first bootstrap");

        let mut mismatch = request(source.path(), &repo_id);
        mismatch.canonical_repository = source.path().join("moved").to_string_lossy().to_string();
        let error = service.bootstrap(mismatch).await.expect_err("mismatch");
        assert!(
            matches!(error, ServiceError::Conflict(message) if message.contains("different repository") || message.contains("data root"))
        );
        assert_eq!(list_projects(&db).await.expect("projects").len(), 1);
    }

    #[tokio::test]
    async fn a_moved_checkout_reuses_the_canonical_repo_binding() {
        let (db, service) = fixture().await;
        let source = tempdir().expect("original repository");
        let moved = tempdir().expect("moved repository");
        let repo_id = db::new_uuid_v4();
        let first = service
            .bootstrap(request(source.path(), &repo_id))
            .await
            .expect("first bootstrap");

        let mut restart = request(source.path(), &repo_id);
        restart.source_path = Some(moved.path().to_string_lossy().to_string());
        let resumed = service
            .bootstrap(restart)
            .await
            .expect("moved checkout resumes");
        assert_eq!(first.project_id, resumed.project_id);
        assert_eq!(first.repo_id, resumed.repo_id);
        let repos = list_project_repos(&db, &first.project_id)
            .await
            .expect("repos");
        assert_eq!(repos.len(), 1);
        assert_eq!(
            repos[0].local_path.as_deref(),
            Some(moved.path().to_string_lossy().as_ref())
        );
    }

    #[tokio::test]
    async fn only_authenticated_supported_cli_candidates_are_eligible() {
        let (db, service) = fixture().await;
        let source = tempdir().expect("temp repository");
        let repo_id = db::new_uuid_v4();
        let initial = service
            .bootstrap(request(source.path(), &repo_id))
            .await
            .expect("owner bootstrap");
        let now = db::now_rfc3339();
        let daemon = db::DaemonRepo::upsert_by_machine_id(
            &*db,
            UpsertDaemon {
                id: db::new_uuid_v4(),
                machine_id: "solo-test-machine".to_owned(),
                hostname: "localhost".to_owned(),
                os: "test".to_owned(),
                arch: "test".to_owned(),
                agent_version: None,
                labels_json: "{}".to_owned(),
                status: DaemonStatus::Online,
                registration_token_hash: None,
                owner_id: Some(initial.owner_id.clone()),
                visibility: "account".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("daemon");
        db::DaemonRepo::update_report(
            &*db,
            UpdateDaemonReport {
                id: daemon.id.clone(),
                last_report_at: now.clone(),
                status: DaemonStatus::Online,
                detected_clis_json: r#"[{"kind":"codex","availability":"authenticated"}]"#
                    .to_owned(),
                labels_json: None,
                updated_at: now.clone(),
            },
        )
        .await
        .expect("daemon report");
        let agent = AgentRepo::create(
            &*db,
            CreateAgent {
                id: db::new_uuid_v4(),
                name: "Codex local".to_owned(),
                description: None,
                executor_type: "codex".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: Some(daemon.id),
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: Some(initial.owner_id.clone()),
                visibility: "account".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("agent");
        let mut confirmed = request(source.path(), &repo_id);
        confirmed.agent_candidates = vec![SoloAgentCandidateInput {
            identity_id: agent.id.clone(),
            profile_id: Some(agent.profile_id.clone()),
            executor_type: "codex".to_owned(),
            availability: SoloAgentAvailability::Authenticated,
            display_name: None,
        }];
        confirmed.selected_project_agent_id = Some(agent.id.clone());
        confirmed.selected_worker_agent_id = Some(agent.id.clone());
        let selected = service
            .bootstrap(confirmed)
            .await
            .expect("selected bootstrap");
        assert_eq!(
            selected.readiness,
            SoloBootstrapReadiness::CharterAdoptionRequired
        );
        assert_eq!(
            selected.selected_project_agent_id.as_deref(),
            Some(agent.id.as_str())
        );
        assert_eq!(
            selected.project_agent_identity_id.as_deref(),
            Some(agent.id.as_str())
        );
        assert_eq!(
            selected.project_agent_profile_id.as_deref(),
            Some(agent.profile_id.as_str())
        );
        assert_eq!(
            selected.worker_identity_id.as_deref(),
            Some(agent.id.as_str())
        );
        assert!(!selected.mutation_authority_granted);

        let chat = AgentChatRepo::get_project_chat(&*db, &selected.project_id)
            .await
            .expect("project chat")
            .expect("canonical project chat");
        assert_eq!(chat.status, PROJECT_CHAT_READY_STATUS);
        let binding =
            ProjectAgentBindingRepo::get_active_project_binding(&*db, &selected.project_id)
                .await
                .expect("project binding")
                .expect("active adoption binding");
        assert_eq!(binding.state, "active");
        assert!(binding.charter_setup_required);
        assert_eq!(
            binding.permission_ceiling_json,
            SOLO_PROJECT_PERMISSION_CEILING_JSON
        );
        assert!(binding.admission_receipt_id.is_none());
        assert!(binding.charter_approval_id.is_none());
        assert!(binding.charter_id.is_none());
        assert!(binding.charter_revision_id.is_none());

        // Selection activates only the existing adoption conversation. The
        // typed Agent Chat admission path can now accept a user message, but
        // the Project remains Charter-gated and therefore has no Task
        // mutation authority.
        let admitted = crate::AgentChatService::new(Arc::clone(&db))
            .send_message(crate::SendAgentChatMessageInput {
                actor_user_id: selected.owner_id.clone(),
                chat_id: selected.project_chat_id.clone(),
                content: "Please explain the Project Charter adoption choices.".to_owned(),
                dedupe_key: Some("solo-adoption-chat-test".to_owned()),
            })
            .await
            .expect("adoption chat message");
        assert_eq!(admitted.message.chat_id, selected.project_chat_id);
        assert!(!selected.mutation_authority_granted);
        assert_eq!(
            selected.readiness,
            SoloBootstrapReadiness::CharterAdoptionRequired
        );

        db::DaemonRepo::update_report(
            &*db,
            UpdateDaemonReport {
                id: agent.daemon_id.expect("daemon id"),
                last_report_at: db::now_rfc3339(),
                status: DaemonStatus::Offline,
                detected_clis_json: "[]".to_owned(),
                labels_json: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .expect("daemon offline");
        let error = service
            .bootstrap({
                let mut retry = request(source.path(), &repo_id);
                retry.agent_candidates = vec![SoloAgentCandidateInput::authenticated(
                    agent.id.clone(),
                    "codex",
                )];
                retry.selected_project_agent_id = Some(agent.id.clone());
                retry.selected_worker_agent_id = Some(agent.id);
                retry
            })
            .await
            .expect_err("unavailable selection must fail closed");
        assert!(
            matches!(error, ServiceError::Conflict(message) if message.contains("unavailable"))
        );
    }

    #[tokio::test]
    async fn authenticated_registration_creates_and_replays_owned_agent_profile() {
        let (db, service) = fixture().await;
        let source = tempdir().expect("temp repository");
        let repo_id = db::new_uuid_v4();
        let initial = service
            .bootstrap(request(source.path(), &repo_id))
            .await
            .expect("owner bootstrap");
        let now = db::now_rfc3339();
        let daemon = db::DaemonRepo::upsert_by_machine_id(
            &*db,
            UpsertDaemon {
                id: db::new_uuid_v4(),
                machine_id: "solo-registration-machine".to_owned(),
                hostname: "localhost".to_owned(),
                os: "test".to_owned(),
                arch: "test".to_owned(),
                agent_version: None,
                labels_json: "{}".to_owned(),
                status: DaemonStatus::Online,
                registration_token_hash: None,
                owner_id: Some(initial.owner_id.clone()),
                visibility: "account".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("daemon");
        db::DaemonRepo::update_report(
            &*db,
            UpdateDaemonReport {
                id: daemon.id.clone(),
                last_report_at: now.clone(),
                status: DaemonStatus::Online,
                detected_clis_json: r#"[{"kind":"codex","availability":"authenticated"}]"#
                    .to_owned(),
                labels_json: None,
                updated_at: now,
            },
        )
        .await
        .expect("daemon report");

        let registration = SoloAuthenticatedAgentRegistration::authenticated(
            "daemon:solo-registration-machine:codex",
            "Codex local",
            "codex",
            Some(daemon.id),
        );
        let first = service
            .ensure_authenticated_agent(&initial.owner_id, registration.clone())
            .await
            .expect("agent registration");
        let second = service
            .ensure_authenticated_agent(&initial.owner_id, registration)
            .await
            .expect("agent registration replay");
        assert!(first.eligible);
        assert_eq!(first.identity_id, second.identity_id);
        assert_eq!(first.profile_id, second.profile_id);
        assert_eq!(list_agents(&db).await.expect("agents").len(), 1);
    }

    #[test]
    fn supported_executor_matrix_excludes_embedded_shell_and_null() {
        assert!(allowed_cli_executor("codex"));
        assert!(allowed_cli_executor("claude_code"));
        assert!(allowed_cli_executor("cursor"));
        assert!(allowed_cli_executor("opencode"));
        assert!(allowed_cli_executor("gemini"));
        assert!(allowed_cli_executor("smith"));
        assert!(!allowed_cli_executor("embedded"));
        assert!(!allowed_cli_executor("shell"));
        assert!(!allowed_cli_executor("null"));
    }
}
