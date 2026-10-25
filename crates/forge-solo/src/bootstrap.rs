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
    SystemSettingRepo, UpdateProject, UpdateRepo, User, UserRepo,
};
use executors::ExecutorKind;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use services::{
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
    embedded_agents: Option<Arc<services_agents::EmbeddedAgentService>>,
    config_providers: config::ProviderDeclarations,
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
        Self {
            db,
            embedded_agents: None,
            config_providers: config::ProviderDeclarations::default(),
        }
    }

    /// Supply the embedded Agent runtime used to materialize config-declared
    /// provider entries and direct Agents.  Without it, config provider
    /// declarations are ignored and Solo keeps its CLI-harness-only candidate
    /// set.
    #[must_use]
    pub fn with_embedded_agents(
        mut self,
        service: Arc<services_agents::EmbeddedAgentService>,
    ) -> Self {
        self.embedded_agents = Some(service);
        self
    }

    /// Supply the provider declarations parsed from the user-owned Forge
    /// config file.  Solo is the consumer: the server keeps its own
    /// UI/API-driven provider onboarding.
    #[must_use]
    pub fn with_config_providers(mut self, providers: config::ProviderDeclarations) -> Self {
        self.config_providers = providers;
        self
    }

    /// Create or resume the one Solo owner/Project/repository/chat setup for
    /// the request's immutable repository identity.
    pub async fn bootstrap(&self, request: SoloBootstrapRequest) -> Result<SoloBootstrapResult> {
        let request = normalize_request(request)?;
        let (idempotency_key, input_digest) = bootstrap_identity(&request)?;
        let workflow = default_autonomous_workflow();
        services::workflow::validation::validate_workflow(&workflow)?;
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

        // Config-declared providers are synced before candidate discovery so
        // the picker can offer direct Agents alongside local CLI harnesses.
        // Each provider syncs independently: an unreachable endpoint or a
        // missing env secret skips that provider instead of failing the
        // bootstrap, and the CLI-harness candidates remain unaffected.
        self.sync_config_provider_agents(&owner_id).await;

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

mod agents;
mod project;

#[cfg(test)]
mod tests;
