//! Durable execution loop for Main and Project Agent Chat turns.
//!
//! Chat jobs intentionally do not share the Task worker's workspace contract.
//! The worker claims one FIFO job per responder/scope, renews an expiring
//! lease while the backend is running, and commits the response through the
//! atomic Agent Chat service composite.  A failed adapter call is persisted on
//! the job with a bounded error and a finite retry budget.

use std::{collections::BTreeSet, fmt, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use db::{
    canonicalize_project_handoff_json, now_rfc3339, project_handoff_request_fingerprint,
    AccountMainAgentBindingRepo, Agent, AgentChatMessage, AgentChatMessageAuthorType,
    AgentChatMessageListQuery, AgentChatMessageRepo, AgentChatMessageStatus, AgentChatRepo,
    AgentChatTransactionRepo, AgentChatTurnJob, AgentChatTurnJobRepo, AgentProfile,
    AgentProfileRepo, AgentRepo, AgentSession, CompleteAgentChatControlTransfer,
    CredentialHandleRepo, PageRequest, ProjectAgentBindingRepo, ProjectRepo, SqliteDb,
};
use executors::{
    merge_overrides, ExecutionContext, ExecutionOutcome, ExecutionOverrides, ExecutionResult,
    ExecutorKind, TaskExecutor,
};
use forge_agent_host::RuntimeContextManifestLink;
use forge_agent_host::{
    AgentSessionBackend, AgentTurnRequest, BackendCapabilities, CanonicalScope, CanonicalScopeType,
    Message, NativeProviderConfig, Role, TurnEventSink, WorkspaceAccess,
    MAIN_GENESIS_START_OPERATION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    agent_chat_policy::guard_agent_chat_content,
    agent_chat_service::{
        AgentChatService, AppendAgentChatSuccessInput, CommittedAgentChatResponse,
    },
    agent_chat_turn_policy::failure_after_claim,
    context_manifest::{ContextManifestInput, ContextManifestService, ContextSourceInput},
    embedded_agent_service::{
        CreateFrozenAgentChatSession, CreateScopedSession, RequestedCanonicalScope,
    },
    operating_skills::{
        canonical_main_baseline_operating_skill_body,
        canonical_main_baseline_operating_skill_body_for_revision,
        canonical_main_operating_skill_body, canonical_project_operating_skill_body,
        render_main_baseline_operating_skill, render_project_operating_skill,
        EffectiveProjectStateContext, MainBaselineSkillContext, ProjectOperatingSkillContext,
        LEGACY_MAIN_BASELINE_OPERATING_SKILL_REVISION,
        MAIN_BASELINE_OPERATING_SKILL_CONTENT_DIGEST, MAIN_BASELINE_OPERATING_SKILL_KEY,
        MAIN_BASELINE_OPERATING_SKILL_REVISION, MAIN_OPERATING_SKILL_CONTENT_DIGEST,
        MAIN_OPERATING_SKILL_KEY, MAIN_OPERATING_SKILL_POLICY_DIGEST,
        MAIN_OPERATING_SKILL_POLICY_JSON, MAIN_OPERATING_SKILL_RENDER_VERSION,
        MAIN_OPERATING_SKILL_SCHEMA_VERSION, PROJECT_OPERATING_SKILL_CONTENT_DIGEST,
        PROJECT_OPERATING_SKILL_KEY, PROJECT_OPERATING_SKILL_POLICY_DIGEST,
        PROJECT_OPERATING_SKILL_POLICY_JSON, PROJECT_OPERATING_SKILL_RENDER_VERSION,
        PROJECT_OPERATING_SKILL_SCHEMA_VERSION,
    },
    project_runtime::{load_effective_project_state, ProjectEffectiveStateProjection},
    wake_turn_consumer::{
        DELIVERY_FOLLOWUP_POSTCONDITION_SCHEMA, DELIVERY_FOLLOWUP_READINESS_EVENT,
    },
    EmbeddedAgentService, Result, ServiceError,
};

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(30);
const TURN_LEASE_SECONDS: i64 = 120;
const MAX_ACTIVE_TURNS: usize = 32;
const MAX_HISTORY: i64 = 100;
const MAX_ERROR_CHARS: usize = 512;
const MAX_CLI_ASSISTANT_CHARS: usize = 500;
const PROJECT_HANDOFF_SCHEMA_VERSION: &str = "forge.project-charter-handoff/v1";
const PROJECT_CONTEXT_DIGEST_SCHEMA_VERSION: &str = "forge.project-context-reference/v1";
const MAX_HANDOFF_BOUNDED_CHARS: usize = 12_000;
const DELIVERY_FOLLOWUP_POSTCONDITION_FAILED: &str = "delivery_followup_postcondition_failed";
const DELIVERY_FOLLOWUP_POSTCONDITION_MESSAGE: &str =
    "Delivery follow-up returned without committing a newer milestone readiness evaluation";
const DELIVERY_FOLLOWUP_RETRY_INSTRUCTION: &str = r#"

## SERVER-OWNED DELIVERY FOLLOW-UP RETRY
Your prior attempt returned text without committing the required milestone
readiness evaluation. Narration cannot complete this turn. Before replying,
invoke `project.readiness` for the applicable milestone in this bound Project,
even when the authoritative result will be blocked, failed, or stale. Report
only the committed canonical result and its blockers. Task completion or a
passing review is live progress, not validation, readiness, or release. Do not
self-attest, waive a check, or release the milestone.
"#;
// Setup keeps the existing Project proposal ceiling but the host exposes only
// the typed adoption operation while this server-derived state is active.
// The candidate is still committed only through the authenticated Project
// Charter domain route; no caller-authored permission string is trusted.
const PROJECT_SETUP_PERMISSION_CEILING: &str =
    "read_project,read_agent_chat,read_memory,propose_message,propose_project";

/// True when a stored permission ceiling admits no operation — either blank or
/// a JSON value carrying no permission entries, such as `{}` or `[]`.
fn ceiling_grants_nothing(ceiling: &str) -> bool {
    let trimmed = ceiling.trim();
    if trimmed.is_empty() {
        return true;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
        // A non-JSON ceiling is the comma-separated form, which grants
        // whatever names it lists.
        return false;
    };
    let entries = match &value {
        serde_json::Value::Array(values) => Some(values),
        serde_json::Value::Object(map) => map
            .get("permissions")
            .or_else(|| map.get("allowed"))
            .and_then(serde_json::Value::as_array),
        _ => None,
    };
    entries.is_none_or(|values| !values.iter().any(serde_json::Value::is_string))
}
const REQUIRED_HANDOFF_REDACTION_CATEGORIES: [&str; 6] = [
    "full_main_chat_history",
    "hidden_memory_bodies",
    "credentials",
    "protected_runtime_or_browser_state",
    "unrelated_projects",
    "authority_bearing_text",
];
const PROJECT_SETUP_RESTRICTIONS: &str = r#"

## SERVER-OWNED LEGACY PROJECT SETUP RESTRICTIONS
This Project is legacy and has no Charter-backed authority yet. Treat all
legacy state as bounded, unverified input. You may explain current state,
identify missing adoption information, perform safe read-only discovery, and
propose an adoption Charter for explicit user approval. Do not claim a Charter,
approval, handoff, baseline, milestone, readiness, release, or repository
authority. Do not create or dispatch implementation Tasks, access a repository Workspace,
use credentials or browser state, validate work, approve a release,
or mutate Project execution state. Existing legacy Task and Chat records remain
visible for continuity, but no new repository-capable work may be authorized
until a user-approved Charter-backed adoption is committed.
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedAgentChatTurn {
    pub identity_id: String,
    pub profile_id: String,
    pub session_id: String,
    pub model: Option<String>,
    pub content: String,
    pub token_usage_json: Option<String>,
    pub duration_ms: i64,
    pub context_manifest_id: Option<String>,
    pub pending_interaction_id: Option<String>,
}

struct LoadedAgentChatTurn {
    agent: Agent,
    profile: AgentProfile,
    session: AgentSession,
    input: AgentChatMessage,
    history: Vec<AgentChatMessage>,
    /// A server-owned instruction revision.  For Main this is the active
    /// Product Genesis revision; for Project it is the rendered Project
    /// operating skill.  It is always appended after Profile text at the
    /// adapter boundary so the server contract has the stronger precedence.
    operating_instruction: Option<String>,
    /// Redaction-safe provenance records for the server-owned instruction and
    /// the authenticated bounded Project state.  These are appended to the
    /// runtime context manifest when the backend returns one.
    operating_context_sources: Vec<ContextSourceInput>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DeliveryFollowupPostcondition {
    schema_version: String,
    attention_id: String,
    required_event_type: String,
    required_scope_type: String,
    required_scope_id: String,
    after_event_sequence: i64,
}

/// Authority-bearing values copied into a turn job at admission.  Binding
/// rows and Profiles are retained as history, but the active binding is not a
/// valid source for an already-admitted job: replacement must never mix a new
/// binding/policy with the old Profile or operating skill.
#[derive(Debug, Clone)]
struct FrozenTurnAuthority {
    binding_id: String,
    binding_version: i64,
    profile_version: i64,
    operating_skill_revision_id: String,
    policy_revision: String,
    policy_digest: String,
    permission_policy_digest: String,
    tool_policy_digest: String,
    operating_skill_key: String,
    operating_skill_body: String,
    operating_skill_content_digest: String,
}

#[derive(Debug, Clone)]
struct ProjectOperatingSkillSnapshot {
    instruction: String,
    context_sources: Vec<ContextSourceInput>,
}

/// A bounded, server-derived context reference.  The display value may be
/// shown in an operating-skill prompt, while the remaining fields are kept in
/// the persisted manifest so a reviewer can tell which exact authority record
/// was selected, why it was selected, and whether it was stale or omitted.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OperatingContextReference {
    display: String,
    source_id: String,
    source_type: String,
    source_revision: String,
    digest: String,
    selection_reason: String,
    disposition: String,
    sensitivity: String,
}

impl OperatingContextReference {
    fn included(
        display: impl Into<String>,
        source_id: impl Into<String>,
        source_type: impl Into<String>,
        source_revision: impl Into<String>,
        digest: impl Into<String>,
        selection_reason: impl Into<String>,
    ) -> Self {
        Self {
            display: display.into(),
            source_id: source_id.into(),
            source_type: source_type.into(),
            source_revision: source_revision.into(),
            digest: digest.into(),
            selection_reason: selection_reason.into(),
            disposition: "included".to_owned(),
            sensitivity: "internal".to_owned(),
        }
    }

    fn omitted(
        display: impl Into<String>,
        source_id: impl Into<String>,
        source_type: impl Into<String>,
        source_revision: impl Into<String>,
        digest: impl Into<String>,
        selection_reason: impl Into<String>,
    ) -> Self {
        Self {
            disposition: "omitted".to_owned(),
            ..Self::included(
                display,
                source_id,
                source_type,
                source_revision,
                digest,
                selection_reason,
            )
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffPacket {
    schema_version: String,
    handoff_id: String,
    deduplication_key: String,
    correlation_id: String,
    causation_id: String,
    /// The approval receipt and request envelope are populated by the atomic
    /// Project-creation transaction. They are required parts of the v1 packet;
    /// accepting their absence would make an incomplete packet look like a
    /// valid handoff.
    approval_id: String,
    request: ProjectCharterHandoffRequest,
    source: ProjectCharterHandoffSource,
    project: ProjectCharterHandoffProject,
    target: ProjectCharterHandoffTarget,
    charter: ProjectCharterHandoffCharter,
    approval: ProjectCharterHandoffApproval,
    project_agent: ProjectCharterHandoffAgent,
    bounded_summary: String,
    settled_decision_ids: Vec<String>,
    unresolved_items: Vec<Value>,
    research_references: Vec<Value>,
    content_classification: String,
    redaction_manifest: ProjectCharterHandoffRedactionManifest,
    created_at: String,
    delivery: ProjectCharterHandoffDelivery,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffSource {
    chat_id: String,
    message_ids: Vec<String>,
    message_id: String,
    turn_id: Option<String>,
    identity_id: String,
    profile_revision_id: String,
    instruction_revision_id: String,
    instruction_revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffRequest {
    policy_revision: String,
    policy_digest: String,
    source_revisions_digest: String,
    source_revisions_json: String,
    authorization: ProjectCharterHandoffAuthorization,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffAuthorization {
    principal_type: String,
    principal_id: String,
    authorization_basis: String,
    action: String,
    event_id: String,
    occurred_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffProject {
    id: String,
    name: String,
    lifecycle: String,
    mode: String,
    approved_slug: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffTarget {
    chat_id: String,
    binding_id: String,
    identity_id: String,
    profile_revision_id: String,
    message_id: String,
    turn_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffCharter {
    id: String,
    revision_id: String,
    revision_number: i64,
    schema_version: String,
    content_digest: String,
    render_version: String,
    render_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffApproval {
    id: String,
    event_id: String,
    authorization_basis: String,
    authorization_action: String,
    authorization_event_id: String,
    authorization_occurred_at: String,
    approved_by: ProjectCharterHandoffPrincipal,
    approved_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffAgent {
    identity_id: String,
    profile_revision_id: String,
    operating_skill_revision: String,
    policy_revision: String,
    policy_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffPrincipal {
    kind: String,
    id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffRedactionManifest {
    excluded_knowledge_item_ids: Vec<String>,
    excluded_categories: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCharterHandoffDelivery {
    delivered_at: String,
}

struct ProjectHandoffExpectation<'a> {
    handoff_id: &'a str,
    deduplication_key: &'a str,
    correlation_id: &'a str,
    causation_id: &'a str,
    source_chat_id: &'a str,
    source_identity_id: &'a str,
    source_profile_revision_id: &'a str,
    source_instruction_revision_id: &'a str,
    source_instruction_revision: i64,
    source_message_ids: Vec<String>,
    source_turn_id: Option<&'a str>,
    project_id: &'a str,
    project_name: &'a str,
    project_mode: &'a str,
    approved_slug: Option<&'a str>,
    target_chat_id: &'a str,
    target_binding_id: &'a str,
    target_message_id: &'a str,
    target_turn_id: &'a str,
    charter_id: &'a str,
    charter_revision_id: &'a str,
    charter_revision_number: i64,
    charter_schema_version: &'a str,
    charter_content_digest: &'a str,
    charter_render_version: &'a str,
    charter_render_digest: &'a str,
    approval_id: &'a str,
    approval_event_id: &'a str,
    approval_authorization_basis: &'a str,
    approval_authorization_action: &'a str,
    approval_authorization_event_id: &'a str,
    approval_authorization_occurred_at: &'a str,
    approval_principal_kind: &'a str,
    approval_principal_id: &'a str,
    approval_created_at: &'a str,
    create_authorization_principal_type: &'a str,
    create_authorization_principal_id: &'a str,
    create_authorization_basis: &'a str,
    create_authorization_action: &'a str,
    create_authorization_event_id: &'a str,
    create_authorization_occurred_at: &'a str,
    identity_id: &'a str,
    profile_revision_id: &'a str,
    policy_revision: &'a str,
    policy_digest: &'a str,
    created_at: &'a str,
    delivered_at: &'a str,
}

#[async_trait]
pub trait AgentChatTurnRunner: Send + Sync {
    async fn run_turn(
        &self,
        job: &AgentChatTurnJob,
        cancellation: CancellationToken,
    ) -> Result<CompletedAgentChatTurn>;
}

/// Narrow legacy CLI adapter for migrated Agent Chats. It deliberately uses a
/// disposable empty directory and advertises denied workspace authority; a
/// Task execution path is not routed through this type.
#[derive(Clone)]
pub struct CliAgentChatSessionBackend {
    executor: Arc<dyn TaskExecutor>,
}

impl fmt::Debug for CliAgentChatSessionBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliAgentChatSessionBackend")
            .finish_non_exhaustive()
    }
}

impl CliAgentChatSessionBackend {
    pub fn new(executor: Arc<dyn TaskExecutor>) -> Self {
        Self { executor }
    }

    pub fn capabilities() -> BackendCapabilities {
        BackendCapabilities {
            native_runtime: false,
            persistent_session: false,
            protected_checkpoints: false,
            lcm: false,
            cancel: true,
            steer: false,
            workspace: WorkspaceAccess::Deny,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run_turn(
        &self,
        scope: &CanonicalScope,
        job_id: &str,
        chat_id: &str,
        executor_type: &str,
        agent_config: Value,
        prompt: String,
        cancellation: CancellationToken,
    ) -> Result<(ExecutionResult, i64)> {
        if scope.scope_type != CanonicalScopeType::AgentChat
            || scope.scope_id != chat_id
            || scope.workspace_access != WorkspaceAccess::Deny
        {
            return Err(ServiceError::invalid_operation(
                "CLI Agent Chat backend requires a denied-filesystem Agent Chat scope",
            ));
        }
        let kind = executor_type
            .parse::<ExecutorKind>()
            .map_err(ServiceError::invalid_operation)?;
        let executor_type = kind.to_string();
        if matches!(kind, ExecutorKind::Shell | ExecutorKind::Embedded) {
            return Err(ServiceError::invalid_operation(
                "selected executor cannot run a legacy CLI Agent Chat turn",
            ));
        }
        let executor_snapshot = cli_executor_snapshot(&executor_type, agent_config);

        let sandbox = chat_sandbox_path(job_id);
        let logs_path = chat_log_path(job_id);
        if sandbox.exists() {
            std::fs::remove_dir_all(&sandbox).map_err(|_| {
                ServiceError::invalid_operation("stale Agent Chat sandbox could not be removed")
            })?;
        }
        std::fs::create_dir_all(&sandbox).map_err(|_| {
            ServiceError::invalid_operation("Agent Chat sandbox could not be created")
        })?;
        if let Some(parent) = logs_path.parent() {
            std::fs::create_dir_all(parent).map_err(|_| {
                ServiceError::invalid_operation("Agent Chat log directory could not be created")
            })?;
        }

        let started = std::time::Instant::now();
        let execution = self.executor.execute(ExecutionContext {
            task_id: chat_id.to_owned(),
            execution_id: job_id.to_owned(),
            worktree_path: sandbox.to_string_lossy().into_owned(),
            description: prompt,
            agent_config: executor_snapshot,
            logs_path: logs_path.to_string_lossy().into_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        });
        tokio::pin!(execution);
        let result = tokio::select! {
            result = &mut execution => result,
            _ = cancellation.cancelled() => {
                let _ = self.executor.cancel(job_id).await;
                let _ = std::fs::remove_dir_all(&sandbox);
                return Err(ServiceError::invalid_operation("Agent Chat CLI turn was cancelled"));
            }
        }?;
        let _ = std::fs::remove_dir_all(&sandbox);
        Ok((result, started.elapsed().as_millis() as i64))
    }
}

#[derive(Clone)]
pub struct FederatedAgentChatTurnRunner {
    db: Arc<SqliteDb>,
    embedded_agents: Arc<EmbeddedAgentService>,
    cli_backend: CliAgentChatSessionBackend,
}

impl fmt::Debug for FederatedAgentChatTurnRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FederatedAgentChatTurnRunner")
            .finish_non_exhaustive()
    }
}

impl FederatedAgentChatTurnRunner {
    pub fn new(
        db: Arc<SqliteDb>,
        embedded_agents: Arc<EmbeddedAgentService>,
        cli_executor: Arc<dyn TaskExecutor>,
    ) -> Self {
        Self {
            db,
            embedded_agents,
            cli_backend: CliAgentChatSessionBackend::new(cli_executor),
        }
    }

    /// Validate and load the authority snapshot captured at turn admission.
    ///
    /// V088 deliberately leaves the provenance columns nullable for rows
    /// created before the cutover.  Such rows take the legacy path and are
    /// still subject to the existing conservative authority checks.  A row
    /// created after the cutover must be complete; falling back to an active
    /// binding, the selected Profile, or the current skill would make a
    /// queued turn change meaning while it was waiting.
    async fn load_frozen_authority(
        &self,
        chat: &db::AgentChat,
        job: &AgentChatTurnJob,
        agent: &Agent,
        profile: &AgentProfile,
    ) -> Result<Option<FrozenTurnAuthority>> {
        let provenance_fields = [
            job.responder_binding_id.is_some(),
            job.responder_binding_version.is_some(),
            job.responder_identity_version.is_some(),
            job.profile_version.is_some(),
            job.operating_skill_revision_id.is_some(),
            job.policy_revision.is_some(),
            job.policy_digest.is_some(),
            job.permission_policy_digest.is_some(),
            job.tool_policy_digest.is_some(),
            job.admission_digest.is_some(),
            job.canonical_scope_provenance_json.is_some(),
        ];
        if provenance_fields.iter().all(|present| !present) {
            return Ok(None);
        }
        if provenance_fields.iter().any(|present| !present) {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn has incomplete admission provenance",
            ));
        }
        if job.canonical_scope_type != "agent_chat" || job.canonical_scope_id != job.chat_id {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn has an invalid frozen canonical scope",
            ));
        }
        let binding_id = job.responder_binding_id.clone().ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen responder binding")
        })?;
        let binding_version = job.responder_binding_version.ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen binding version")
        })?;
        let profile_version = job.profile_version.ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen Profile version")
        })?;
        let identity_version = job.responder_identity_version.ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen identity version")
        })?;
        if profile.version != profile_version {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn Profile revision is no longer available",
            ));
        }
        if job.profile_id.as_deref() != Some(profile.id.as_str())
            || job.responder_identity_id.as_deref() != Some(agent.id.as_str())
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn frozen responder does not match its Profile",
            ));
        }

        // The provenance JSON is redaction-safe, but it is still checked as a
        // second representation of the admission receipt.  This catches a
        // partially migrated row or an accidental column mix before any
        // session/backend work occurs.
        let provenance =
            serde_json::from_str::<crate::agent_turn_admission::ResolvedAgentResponder>(
                job.canonical_scope_provenance_json
                    .as_deref()
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Agent Chat turn has no canonical scope provenance",
                        )
                    })?,
            )
            .map_err(|_| {
                ServiceError::invalid_operation("Agent Chat turn canonical provenance is invalid")
            })?;
        provenance.require_frozen()?;
        if provenance.chat_id != chat.id
            || provenance.canonical_scope_type != job.canonical_scope_type
            || provenance.canonical_scope_id != job.canonical_scope_id
            || provenance.binding_id.as_deref() != Some(binding_id.as_str())
            || provenance.binding_version != Some(binding_version)
            || provenance.identity_id.as_deref() != Some(agent.id.as_str())
            || provenance.identity_version != Some(identity_version)
            || provenance.profile_id.as_deref() != Some(profile.id.as_str())
            || provenance.profile_version != Some(profile_version)
            || provenance.operating_skill_revision.as_deref()
                != job.operating_skill_revision_id.as_deref()
            || provenance.policy_revision.as_deref() != job.policy_revision.as_deref()
            || provenance.policy_digest.as_deref() != job.policy_digest.as_deref()
            || provenance.permission_policy_digest.as_deref()
                != job.permission_policy_digest.as_deref()
            || provenance.tool_policy_digest.as_deref() != job.tool_policy_digest.as_deref()
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn frozen provenance representations disagree",
            ));
        }

        let (
            binding_identity_id,
            policy_json,
            permission_json,
            policy_revision,
            stored_policy_digest,
            binding_state,
            binding_row_version,
            expected_skill_key,
        ) = match chat.kind.as_str() {
            "account_main" => {
                let account_id = chat.account_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation("Main Agent Chat has no account scope")
                })?;
                let binding = AccountMainAgentBindingRepo::get_main_binding(&*self.db, &binding_id)
                    .await?
                    .filter(|binding| binding.account_id == account_id)
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Agent Chat frozen Main binding history is unavailable",
                        )
                    })?;
                (
                    binding.identity_id,
                    binding.autonomy_policy_json,
                    // Main account ceilings are mutable command-time policy.
                    // The admission digest is retained below for provenance,
                    // but it must not turn a later tightening into a reason
                    // to reject an otherwise frozen model turn.
                    String::new(),
                    binding.tool_policy_revision,
                    None,
                    binding.state,
                    binding.version,
                    main_skill_key_for_frozen_revision(
                        job.operating_skill_revision_id
                            .as_deref()
                            .unwrap_or_default(),
                    ),
                )
            }
            "project" => {
                let project_id = chat.project_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation("Project Agent Chat has no Project scope")
                })?;
                let row = sqlx::query(
                    "SELECT identity_id, profile_id, state, version, autonomy_policy_json,
                            permission_ceiling_json,
                            operating_skill_revision_id, policy_revision, policy_digest
                     FROM project_agent_binding
                     WHERE id = ? AND project_id = ?",
                )
                .bind(&binding_id)
                .bind(project_id)
                .fetch_optional(self.db.pool())
                .await?
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Agent Chat frozen Project binding history is unavailable",
                    )
                })?;
                let identity_id: Option<String> = row.try_get("identity_id")?;
                let binding_row_version: i64 = row.try_get("version")?;
                let autonomy_policy_json: String = row.try_get("autonomy_policy_json")?;
                let permission_json: String = row.try_get("permission_ceiling_json")?;
                let policy_revision: String = row.try_get("policy_revision")?;
                let stored_policy_digest: String = row.try_get("policy_digest")?;
                (
                    identity_id.ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Agent Chat frozen Project binding has no identity",
                        )
                    })?,
                    autonomy_policy_json,
                    permission_json,
                    policy_revision,
                    Some(stored_policy_digest),
                    row.try_get("state")?,
                    binding_row_version,
                    PROJECT_OPERATING_SKILL_KEY,
                )
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat has an unsupported canonical kind",
                ));
            }
        };

        let historical_binding_version_matches = binding_row_version == binding_version
            || (binding_state == "replaced" && binding_row_version == binding_version + 1);
        if binding_identity_id != agent.id || !historical_binding_version_matches {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen binding does not match its responder",
            ));
        }
        // A replaced binding is valid history for its already-admitted job.
        // A paused/revoked/setup row is an explicit authority withdrawal and
        // must fail closed without selecting a newer active binding.
        if matches!(
            binding_state.as_str(),
            "paused" | "revoked" | "agent_setup_required"
        ) {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen responder binding is no longer executable",
            ));
        }
        if let Some(expected) = job.policy_revision.as_deref() {
            if expected != policy_revision {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat frozen policy revision is no longer available",
                ));
            }
        }
        let computed_policy_digest = crate::agent_turn_admission::policy_digest(&policy_json)?;
        let expected_policy_digest = job.policy_digest.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("Agent Chat turn has no frozen policy digest")
        })?;
        if chat.kind == "account_main" {
            if expected_policy_digest != computed_policy_digest {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat frozen Main policy changed after admission",
                ));
            }
        } else if stored_policy_digest.as_deref() != Some(expected_policy_digest) {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen Project policy changed after admission",
            ));
        }
        let expected_permission_policy_digest =
            job.permission_policy_digest.as_deref().ok_or_else(|| {
                ServiceError::invalid_operation("Agent Chat turn has no frozen permission digest")
            })?;
        if expected_permission_policy_digest.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn has an empty frozen permission digest",
            ));
        }
        if chat.kind == "project"
            && crate::agent_turn_admission::policy_digest(&permission_json)?
                != expected_permission_policy_digest
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen permission policy changed after admission",
            ));
        }
        let permission_policy_digest = expected_permission_policy_digest.to_owned();
        let tool_policy_digest =
            crate::agent_turn_admission::policy_digest(&profile.tool_policy_json)?;
        if job.tool_policy_digest.as_deref() != Some(tool_policy_digest.as_str()) {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen Profile policy changed after admission",
            ));
        }

        let operating_skill_revision_id =
            job.operating_skill_revision_id.clone().ok_or_else(|| {
                ServiceError::invalid_operation("Agent Chat turn has no operating-skill provenance")
            })?;
        if expected_skill_key == MAIN_BASELINE_OPERATING_SKILL_KEY {
            let (operating_skill_body, operating_skill_content_digest) =
                canonical_main_baseline_operating_skill_body_for_revision(
                    &operating_skill_revision_id,
                )
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Agent Chat frozen Main baseline skill does not match a supported Main context",
                    )
                })?;
            return Ok(Some(FrozenTurnAuthority {
                binding_id,
                binding_version,
                profile_version,
                operating_skill_revision_id,
                policy_revision,
                policy_digest: expected_policy_digest.to_owned(),
                permission_policy_digest,
                tool_policy_digest,
                operating_skill_key: MAIN_BASELINE_OPERATING_SKILL_KEY.to_owned(),
                operating_skill_body: operating_skill_body.to_owned(),
                operating_skill_content_digest: operating_skill_content_digest.to_owned(),
            }));
        }
        let skill_row = sqlx::query(
            "SELECT id, skill_key, canonical_body, content_digest
             FROM operating_skill_revision WHERE id = ?",
        )
        .bind(&operating_skill_revision_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "Agent Chat frozen operating-skill revision is no longer available",
            )
        })?;
        let skill_id: String = skill_row.try_get("id")?;
        let skill_key: String = skill_row.try_get("skill_key")?;
        let skill_body: String = skill_row.try_get("canonical_body")?;
        let skill_content_digest: String = skill_row.try_get("content_digest")?;
        if skill_id != operating_skill_revision_id
            || skill_key != expected_skill_key
            || skill_body.trim().is_empty()
            || skill_content_digest.trim().is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat frozen operating-skill revision is invalid",
            ));
        }

        Ok(Some(FrozenTurnAuthority {
            binding_id,
            binding_version,
            profile_version,
            operating_skill_revision_id,
            policy_revision,
            policy_digest: expected_policy_digest.to_owned(),
            permission_policy_digest,
            tool_policy_digest,
            operating_skill_key: skill_key,
            operating_skill_body: skill_body,
            operating_skill_content_digest: skill_content_digest,
        }))
    }

    async fn load_turn(&self, job: &AgentChatTurnJob) -> Result<LoadedAgentChatTurn> {
        if job.canonical_scope_type != "agent_chat" || job.canonical_scope_id != job.chat_id {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn has a mismatched canonical scope",
            ));
        }
        let input =
            AgentChatMessageRepo::get_agent_chat_message(&*self.db, &job.triggering_message_id)
                .await?
                .filter(|message| message.chat_id == job.chat_id)
                .ok_or_else(|| {
                    ServiceError::not_found("agent_chat_message", job.triggering_message_id.clone())
                })?;
        let chat = AgentChatRepo::get_agent_chat(&*self.db, &job.chat_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat", job.chat_id.clone()))?;
        let identity_id = job
            .responder_identity_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat job has no responder"))?;
        let agent = AgentRepo::get_by_id(&*self.db, identity_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_identity", identity_id.to_owned()))?;
        let profile_id = job
            .profile_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat job has no profile"))?;
        let profile = AgentProfileRepo::get_profile(&*self.db, profile_id)
            .await?
            .filter(|profile| profile.identity_id == agent.id)
            .ok_or_else(|| ServiceError::not_found("agent_profile", profile_id.to_owned()))?;
        let frozen_authority = self
            .load_frozen_authority(&chat, job, &agent, &profile)
            .await?;

        // The Project operating skill is admitted before session creation or
        // any model call.  A Project Chat is never allowed to fall back to a
        // profile-only prompt, an old handoff overlay, or a legacy Project
        // binding when its Charter pointers cannot be proven current.
        let (mut operating_instruction, operating_context_sources) = match chat.kind.as_str() {
            "account_main" => {
                if chat.project_id.is_some() {
                    return Err(ServiceError::invalid_operation(
                        "Main Agent Chat unexpectedly has a Project scope",
                    ));
                }
                // A complete job chooses its Main branch from the persisted
                // skill revision, not from a lifecycle read taken after the
                // job was admitted.  Historical Genesis instructions are
                // immutable; constrain the lookup to rows that existed by
                // admission so a later Genesis cannot replace the context.
                let frozen_skill_key = frozen_authority
                    .as_ref()
                    .map(|snapshot| snapshot.operating_skill_key.as_str());
                let frozen_baseline = frozen_skill_key == Some(MAIN_BASELINE_OPERATING_SKILL_KEY);
                let frozen_genesis = frozen_skill_key == Some(MAIN_OPERATING_SKILL_KEY);
                if frozen_authority.is_some() && !frozen_baseline && !frozen_genesis {
                    return Err(ServiceError::invalid_operation(
                        "Agent Chat frozen Main skill branch is unsupported",
                    ));
                }
                let instruction_row = if frozen_baseline {
                    None
                } else if frozen_genesis {
                    sqlx::query(
                        "SELECT instruction.id AS instruction_id, instruction.revision,
                                instruction.body,
                                genesis.id AS genesis_id, genesis.version AS genesis_version,
                                genesis.lifecycle AS genesis_lifecycle,
                                genesis.charter_id, genesis.charter_revision_id,
                                revision.content_digest AS charter_content_digest,
                                revision.rendered_digest AS charter_render_digest
                         FROM agent_chat_instruction_revision AS instruction
                         JOIN product_genesis_session AS genesis
                           ON genesis.id = instruction.source_id
                          AND genesis.main_chat_id = instruction.chat_id
                         LEFT JOIN project_charter_revision AS revision
                           ON revision.id = genesis.charter_revision_id
                         WHERE instruction.chat_id = ?
                           AND instruction.source_type = 'native'
                           AND instruction.created_at <= ?
                           AND genesis.lifecycle IN (
                               'discovering', 'ready_for_project', 'handed_off', 'cancelled'
                           )
                         ORDER BY instruction.revision DESC, instruction.id DESC
                         LIMIT 1",
                    )
                    .bind(&job.chat_id)
                    .bind(&job.created_at)
                    .fetch_optional(self.db.pool())
                    .await?
                } else {
                    sqlx::query(
                        "SELECT instruction.id AS instruction_id, instruction.revision,
                                instruction.body,
                                genesis.id AS genesis_id, genesis.version AS genesis_version,
                                genesis.lifecycle AS genesis_lifecycle,
                                genesis.charter_id, genesis.charter_revision_id,
                                revision.content_digest AS charter_content_digest,
                                revision.rendered_digest AS charter_render_digest
                         FROM agent_chat_instruction_revision AS instruction
                         JOIN product_genesis_session AS genesis
                           ON genesis.id = instruction.source_id
                          AND genesis.main_chat_id = instruction.chat_id
                         LEFT JOIN project_charter_revision AS revision
                           ON revision.id = genesis.charter_revision_id
                         WHERE instruction.chat_id = ?
                           AND instruction.source_type = 'native'
                           AND genesis.lifecycle IN ('discovering', 'ready_for_project')
                         ORDER BY instruction.revision DESC, instruction.id DESC
                         LIMIT 1",
                    )
                    .bind(&job.chat_id)
                    .fetch_optional(self.db.pool())
                    .await?
                };
                if let Some(instruction_row) = instruction_row {
                    let instruction_id: String = instruction_row.try_get("instruction_id")?;
                    let instruction_revision: i64 = instruction_row.try_get("revision")?;
                    let instruction_body: String = instruction_row.try_get("body")?;
                    let genesis_id: String = instruction_row.try_get("genesis_id")?;
                    let genesis_version: i64 = instruction_row.try_get("genesis_version")?;
                    let genesis_lifecycle: String = instruction_row.try_get("genesis_lifecycle")?;
                    let main_skill_row = if let Some(frozen) = frozen_authority.as_ref() {
                        sqlx::query(
                            "SELECT sr.id AS current_revision_id,
                                    sr.id, sr.skill_key, sr.revision,
                                    sr.schema_version, sr.render_version,
                                    sr.canonical_body, sr.policy_json,
                                    sr.policy_digest, sr.content_digest
                             FROM operating_skill_revision AS sr
                             WHERE sr.id = ?",
                        )
                        .bind(&frozen.operating_skill_revision_id)
                        .fetch_optional(self.db.pool())
                        .await?
                    } else {
                        sqlx::query(
                            "SELECT os.current_revision_id,
                                    sr.id, sr.skill_key, sr.revision,
                                    sr.schema_version, sr.render_version,
                                    sr.canonical_body, sr.policy_json,
                                    sr.policy_digest, sr.content_digest
                             FROM operating_skill AS os
                             JOIN operating_skill_revision AS sr
                               ON sr.id = os.current_revision_id
                             WHERE os.id = ? AND os.lifecycle = 'active'",
                        )
                        .bind(MAIN_OPERATING_SKILL_KEY)
                        .fetch_optional(self.db.pool())
                        .await?
                    }
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Main Agent Genesis has no active server operating-skill revision",
                        )
                    })?;
                    let main_skill_revision: String =
                        main_skill_row.try_get("current_revision_id")?;
                    let main_skill_key: String = main_skill_row.try_get("skill_key")?;
                    let main_skill_revision_number: i64 = main_skill_row.try_get("revision")?;
                    let main_skill_schema_version: String =
                        main_skill_row.try_get("schema_version")?;
                    let main_skill_render_version: String =
                        main_skill_row.try_get("render_version")?;
                    let main_skill_canonical_body: String =
                        main_skill_row.try_get("canonical_body")?;
                    let main_skill_policy_json: String = main_skill_row.try_get("policy_json")?;
                    let main_skill_policy_digest: String =
                        main_skill_row.try_get("policy_digest")?;
                    let main_skill_content_digest: String =
                        main_skill_row.try_get("content_digest")?;
                    let historical_skill_valid = frozen_authority.as_ref().is_some_and(|frozen| {
                        main_skill_revision == frozen.operating_skill_revision_id
                            && main_skill_key == frozen.operating_skill_key
                            && main_skill_revision_number >= 1
                            && !main_skill_schema_version.trim().is_empty()
                            && !main_skill_render_version.trim().is_empty()
                            && !main_skill_canonical_body.trim().is_empty()
                            && !main_skill_policy_json.trim().is_empty()
                            && !main_skill_policy_digest.trim().is_empty()
                            && main_skill_content_digest == frozen.operating_skill_content_digest
                    });
                    let current_skill_valid = frozen_authority.is_none()
                        && main_skill_key == MAIN_OPERATING_SKILL_KEY
                        && main_skill_revision_number >= 1
                        && main_skill_revision
                            == format!("{MAIN_OPERATING_SKILL_KEY}@{main_skill_revision_number}")
                        && main_skill_schema_version == MAIN_OPERATING_SKILL_SCHEMA_VERSION
                        && main_skill_render_version == MAIN_OPERATING_SKILL_RENDER_VERSION
                        && main_skill_canonical_body == canonical_main_operating_skill_body()
                        && main_skill_policy_json == MAIN_OPERATING_SKILL_POLICY_JSON
                        && main_skill_policy_digest == MAIN_OPERATING_SKILL_POLICY_DIGEST
                        && main_skill_content_digest == MAIN_OPERATING_SKILL_CONTENT_DIGEST;
                    if !historical_skill_valid && !current_skill_valid {
                        return Err(ServiceError::invalid_operation(
                            "Main Agent Genesis operating skill does not match this server \
                             build's canonical contract; restart the Forge server so pending \
                             migrations reseed the operating skill",
                        ));
                    }
                    let charter_id: Option<String> = instruction_row.try_get("charter_id")?;
                    let charter_revision_id: Option<String> =
                        instruction_row.try_get("charter_revision_id")?;
                    let charter_content_digest: Option<String> =
                        instruction_row.try_get("charter_content_digest")?;
                    let charter_render_digest: Option<String> =
                        instruction_row.try_get("charter_render_digest")?;
                    let mut references = vec![
                        OperatingContextReference::included(
                            format!("genesis:{genesis_id}@v{genesis_version}"),
                            &genesis_id,
                            "main_genesis",
                            format!("v{genesis_version}:{genesis_lifecycle}"),
                            fingerprint_id(&format!(
                                "genesis:{genesis_id}:v{genesis_version}:lifecycle:{genesis_lifecycle}"
                            )),
                            "active_product_genesis",
                        ),
                        OperatingContextReference::included(
                            format!("main_profile:{}@v{}", profile.id, profile.version),
                            &profile.id,
                            "main_profile",
                            format!("v{}", profile.version),
                            fingerprint_id(&format!(
                                "profile:{}:v{}:policy:{}",
                                profile.id, profile.version, profile.tool_policy_json
                            )),
                            "authenticated_main_profile",
                        ),
                        OperatingContextReference::included(
                            format!("main_instruction:{instruction_id}@{instruction_revision}"),
                            &instruction_id,
                            "main_genesis_instruction",
                            instruction_revision.to_string(),
                            fingerprint_id(&instruction_body),
                            "active_genesis_instruction",
                        ),
                    ];
                    let account_id = chat.account_id.as_deref().ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Main Agent Chat has no authenticated account for portfolio context",
                        )
                    })?;
                    if agent.owner_id.as_deref() != Some(account_id) {
                        return Err(ServiceError::invalid_operation(
                            "Main Agent identity is not owned by the Chat account",
                        ));
                    }
                    references.extend(self.load_main_portfolio_context(account_id).await?);
                    if let (Some(charter_id), Some(charter_revision_id)) =
                        (charter_id.as_deref(), charter_revision_id.as_deref())
                    {
                        if let (Some(content_digest), Some(render_digest)) = (
                            charter_content_digest.as_deref(),
                            charter_render_digest.as_deref(),
                        ) {
                            references.push(OperatingContextReference::included(
                                format!(
                                    "charter:{charter_id}@{charter_revision_id}:content:{content_digest}:render:{render_digest}"
                                ),
                                charter_id,
                                "main_charter",
                                charter_revision_id,
                                content_digest,
                                "active_genesis_charter",
                            ));
                        }
                    }
                    (
                        Some(instruction_body),
                        main_operating_context_sources(
                            frozen_authority
                                .as_ref()
                                .map(|snapshot| snapshot.operating_skill_key.as_str())
                                .unwrap_or(MAIN_OPERATING_SKILL_KEY),
                            &main_skill_revision,
                            &main_skill_content_digest,
                            "server_owned_main_genesis_operating_skill",
                            "main_genesis_context",
                            &references,
                        ),
                    )
                } else {
                    let active_genesis_id = if frozen_authority.is_some() {
                        None
                    } else {
                        sqlx::query_scalar::<_, Option<String>>(
                            "SELECT id
                             FROM product_genesis_session
                             WHERE main_chat_id = ?
                               AND lifecycle IN ('discovering', 'ready_for_project')
                             ORDER BY version DESC, id DESC
                             LIMIT 1",
                        )
                        .bind(&job.chat_id)
                        .fetch_optional(self.db.pool())
                        .await?
                        .flatten()
                    };
                    if active_genesis_id.is_some() {
                        return Err(ServiceError::invalid_operation(
                            "Active Product Genesis has no immutable Main instruction revision",
                        ));
                    }
                    // Outside Genesis the server-owned account baseline is the
                    // operating instruction, so a Main turn never reaches a
                    // model backend without knowing it is Forge's Main Agent.
                    let account_id = chat.account_id.as_deref().ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Main Agent Chat has no authenticated account for baseline context",
                        )
                    })?;
                    if agent.owner_id.as_deref() != Some(account_id) {
                        return Err(ServiceError::invalid_operation(
                            "Main Agent identity is not owned by the Chat account",
                        ));
                    }
                    let mut references = vec![OperatingContextReference::included(
                        format!("main_profile:{}@v{}", profile.id, profile.version),
                        &profile.id,
                        "main_profile",
                        format!("v{}", profile.version),
                        fingerprint_id(&format!(
                            "profile:{}:v{}:policy:{}",
                            profile.id, profile.version, profile.tool_policy_json
                        )),
                        "authenticated_main_profile",
                    )];
                    references.extend(self.load_main_portfolio_context(account_id).await?);
                    let instruction = replace_rendered_skill_body(
                        render_main_baseline_operating_skill(&MainBaselineSkillContext {
                            portfolio_references: references
                                .iter()
                                .filter(|reference| {
                                    reference.source_type == "main_portfolio_projection"
                                })
                                .map(|reference| reference.display.clone())
                                .collect(),
                            profile_text: profile.prompt_template.clone().unwrap_or_default(),
                        }),
                        frozen_authority
                            .as_ref()
                            .map(|snapshot| snapshot.operating_skill_body.as_str()),
                        canonical_main_baseline_operating_skill_body(),
                    )?;
                    let (skill_key, skill_revision, skill_content_digest) = frozen_authority
                        .as_ref()
                        .map(|snapshot| {
                            (
                                snapshot.operating_skill_key.as_str(),
                                snapshot.operating_skill_revision_id.as_str(),
                                snapshot.operating_skill_content_digest.as_str(),
                            )
                        })
                        .unwrap_or((
                            MAIN_BASELINE_OPERATING_SKILL_KEY,
                            MAIN_BASELINE_OPERATING_SKILL_REVISION,
                            MAIN_BASELINE_OPERATING_SKILL_CONTENT_DIGEST,
                        ));
                    (
                        Some(instruction),
                        main_operating_context_sources(
                            skill_key,
                            skill_revision,
                            skill_content_digest,
                            "server_owned_main_baseline_operating_skill",
                            "main_baseline_context",
                            &references,
                        ),
                    )
                }
            }
            "project" => {
                let project_id = chat.project_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project Agent Chat has no authenticated Project scope",
                    )
                })?;
                let snapshot = self
                    .load_project_operating_skill(
                        project_id,
                        &agent,
                        &profile,
                        &chat.id,
                        frozen_authority.as_ref(),
                    )
                    .await?;
                (Some(snapshot.instruction), snapshot.context_sources)
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat has an unsupported canonical kind",
                ));
            }
        };
        append_delivery_followup_retry_instruction(
            job.error_code.as_deref(),
            delivery_followup_postcondition(&input)?.is_some(),
            &mut operating_instruction,
        )?;
        let owner_user_id = agent
            .owner_id
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("Agent identity has no owner"))?;
        let scope = RequestedCanonicalScope::AgentChat {
            chat_id: job.chat_id.clone(),
        };
        let session = if let Some(frozen) = frozen_authority.as_ref() {
            self.embedded_agents
                .create_or_resume_frozen_chat_session(CreateFrozenAgentChatSession {
                    actor_user_id: owner_user_id,
                    identity_id: agent.id.clone(),
                    profile_id: profile.id.clone(),
                    profile_version: frozen.profile_version,
                    binding_id: frozen.binding_id.clone(),
                    binding_version: frozen.binding_version,
                    policy_revision: frozen.policy_revision.clone(),
                    policy_digest: frozen.policy_digest.clone(),
                    permission_policy_digest: frozen.permission_policy_digest.clone(),
                    tool_policy_digest: frozen.tool_policy_digest.clone(),
                    scope,
                })
                .await?
        } else {
            self.embedded_agents
                .create_or_resume_session(CreateScopedSession {
                    actor_user_id: owner_user_id,
                    identity_id: agent.id.clone(),
                    profile_id: Some(profile.id.clone()),
                    scope,
                })
                .await?
        };
        // D21/F18: a new Main turn's episodic context is bounded to the
        // current topic's messages, not the whole prior chat history. A
        // topic-less chat (every Project Chat today, and any Main Chat
        // before its first topic exists) has no floor, so this is a no-op
        // for them.
        let topic_floor =
            db::AgentChatTopicRepo::get_current_agent_chat_topic(&*self.db, &job.chat_id)
                .await?
                .map(|topic| topic.starting_message_sequence)
                .unwrap_or(0);
        let history = AgentChatMessageRepo::list_agent_chat_messages(
            &*self.db,
            AgentChatMessageListQuery {
                chat_id: job.chat_id.clone(),
                before_sequence: Some(input.sequence),
                page: PageRequest {
                    cursor: None,
                    limit: MAX_HISTORY,
                    include_total: false,
                    sort_by: db::SortBy::CreatedAt,
                    sort_order: db::SortOrder::Asc,
                },
            },
        )
        .await?
        .items
        .into_iter()
        .filter(|message| {
            message.sequence < input.sequence
                && message.sequence >= topic_floor
                && message.status == AgentChatMessageStatus::Complete
        })
        .collect();
        Ok(LoadedAgentChatTurn {
            agent,
            profile,
            session,
            input,
            history,
            operating_instruction,
            operating_context_sources,
        })
    }

    /// Load only the account owner's bounded portfolio projection for Main
    /// context. Project rows are represented by stable identifiers, current
    /// versions, and safe lifecycle pointers; names and other mutable detail
    /// remain behind the typed `portfolio.read` projection. No Project Chat,
    /// Task, repository, memory, or credential data enters Main context.
    async fn load_main_portfolio_context(
        &self,
        account_id: &str,
    ) -> Result<Vec<OperatingContextReference>> {
        let rows = sqlx::query(
            "SELECT id, version, updated_at, paused_at, charter_status,
                    current_charter_revision_id
             FROM project
             WHERE owner_id = ?
             ORDER BY updated_at DESC, id DESC
             LIMIT 20",
        )
        .bind(account_id)
        .fetch_all(self.db.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                let project_id: String = row.try_get("id")?;
                let version: i64 = row.try_get("version")?;
                let updated_at: String = row.try_get("updated_at")?;
                let paused = row.try_get::<Option<String>, _>("paused_at")?.is_some();
                let charter_status: String = row.try_get("charter_status")?;
                let charter_revision_id: Option<String> =
                    row.try_get("current_charter_revision_id")?;
                let digest = canonical_context_digest(&serde_json::json!({
                    "project_id": &project_id,
                    "version": version,
                    "updated_at": &updated_at,
                    "paused": paused,
                    "charter_status": &charter_status,
                    "current_charter_revision_id": &charter_revision_id,
                }))?;
                Ok(OperatingContextReference::included(
                    format!("portfolio:{project_id}@v{version}"),
                    project_id,
                    "main_portfolio_projection",
                    format!("v{version}:{updated_at}"),
                    digest,
                    "bounded_account_portfolio_projection",
                ))
            })
            .collect()
    }

    /// Load and authenticate the complete Project operating-skill boundary.
    ///
    /// The worker intentionally does not trust the chat row, handoff prose, or
    /// Profile as an authority source.  The active binding, Project pointers,
    /// approved Charter revision, approval receipt, and selected server skill
    /// are joined and compared here before a session is admitted to a model
    /// backend.  A missing or stale pointer is an error rather than a degraded
    /// prompt: a Project Agent must not make a mutating proposal against an
    /// unproven Project state.
    async fn load_project_operating_skill(
        &self,
        project_id: &str,
        agent: &Agent,
        profile: &AgentProfile,
        project_chat_id: &str,
        frozen: Option<&FrozenTurnAuthority>,
    ) -> Result<ProjectOperatingSkillSnapshot> {
        // Authenticate the Project Agent binding before retrieving any
        // Project row.  The chat's project_id is a routing hint, not an ACL
        // grant, so cross-scope rows must never become a lookup oracle.
        let binding = if let Some(frozen) = frozen {
            ProjectAgentBindingRepo::get_project_binding(&*self.db, &frozen.binding_id)
                .await?
                .filter(|binding| binding.project_id == project_id)
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project Agent turn has no historical admitted Project binding",
                    )
                })?
        } else {
            ProjectAgentBindingRepo::get_active_project_binding(&*self.db, project_id)
                .await?
                .filter(|binding| {
                    // Legacy rows have no frozen binding.  Keep the old
                    // conservative path, which requires the active binding
                    // to still name the admitted identity.
                    binding.state == "active"
                        && binding.identity_id.as_deref() == Some(agent.id.as_str())
                })
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project Agent turn has no authenticated active Project binding",
                    )
                })?
        };
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        // A ceiling of `{}` is textually present but grants nothing, which
        // would hand the model a turn with no tools bound — the agent then
        // narrates work it cannot perform instead of failing. Reject it here.
        if ceiling_grants_nothing(&binding.permission_ceiling_json) {
            return Err(ServiceError::invalid_operation(
                "Project Agent binding has no permission ceiling",
            ));
        }

        // These columns were added by V076 and are intentionally read as a
        // narrow row here until the orchestration repositories expose their
        // full typed binding record.  Reading the exact row also ensures a
        // stale Rust model cannot accidentally turn a missing column into a
        // permissive default.
        let binding_row = sqlx::query(
            "SELECT id, project_id, state, version, identity_id,
                    operating_skill_revision_id, policy_revision, policy_digest,
                    charter_id, charter_revision_id, charter_setup_required
             FROM project_agent_binding
             WHERE id = ? AND project_id = ?",
        )
        .bind(&binding.id)
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent binding disappeared while admitting the turn",
            )
        })?;
        let binding_state: String = binding_row.try_get("state")?;
        let binding_version: i64 = binding_row.try_get("version")?;
        let binding_project_id: String = binding_row.try_get("project_id")?;
        let binding_identity_id: Option<String> = binding_row.try_get("identity_id")?;
        let binding_skill_revision_id: Option<String> =
            binding_row.try_get("operating_skill_revision_id")?;
        let binding_policy_revision: String = binding_row.try_get("policy_revision")?;
        let binding_policy_digest: String = binding_row.try_get("policy_digest")?;
        let binding_charter_id: Option<String> = binding_row.try_get("charter_id")?;
        let binding_charter_revision_id: Option<String> =
            binding_row.try_get("charter_revision_id")?;
        let binding_charter_setup_required: i64 = binding_row.try_get("charter_setup_required")?;
        if let Some(frozen) = frozen {
            let historical_binding_version_matches = binding_version == frozen.binding_version
                || (binding_state == "replaced" && binding_version == frozen.binding_version + 1);
            if !historical_binding_version_matches
                || binding_skill_revision_id.as_deref()
                    != Some(frozen.operating_skill_revision_id.as_str())
                || binding_policy_revision != frozen.policy_revision
                || binding_policy_digest != frozen.policy_digest
            {
                return Err(ServiceError::invalid_operation(
                    "Project Agent historical binding no longer matches turn admission",
                ));
            }
        }

        // Existing Projects intentionally remain usable while they are being
        // adopted into the Charter model.  Their Project Agent may only read
        // the bounded legacy state and propose the adoption Charter; this
        // branch never claims Charter/handoff authority and never enables
        // repository-capable work or release operations.
        if project.charter_status == "legacy_unverified" && project.charter_setup_required {
            if binding_state != "active"
                || binding_project_id != project_id
                || binding_identity_id.as_deref() != Some(agent.id.as_str())
                || binding_charter_setup_required != 1
            {
                return Err(ServiceError::invalid_operation(
                    "Legacy Project Agent setup binding is not authenticated",
                ));
            }
            let skill_row = sqlx::query(
                "SELECT sr.id, sr.skill_key, sr.revision,
                        sr.schema_version, sr.render_version, sr.canonical_body,
                        sr.policy_json, sr.policy_digest, sr.content_digest,
                        os.current_revision_id
                 FROM operating_skill_revision AS sr
                 JOIN operating_skill AS os ON os.id = sr.operating_skill_id
                  AND os.current_revision_id = sr.id
                 WHERE os.lifecycle = 'active' AND sr.skill_key = ?
                 ORDER BY sr.revision DESC, sr.id DESC LIMIT 1",
            )
            .bind(PROJECT_OPERATING_SKILL_KEY)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project setup cannot load the server-owned operating skill",
                )
            })?;
            let setup_skill_revision_id: String = skill_row.try_get("id")?;
            let setup_skill_key: String = skill_row.try_get("skill_key")?;
            let setup_skill_revision: i64 = skill_row.try_get("revision")?;
            let setup_skill_schema_version: String = skill_row.try_get("schema_version")?;
            let setup_skill_render_version: String = skill_row.try_get("render_version")?;
            let setup_skill_canonical_body: String = skill_row.try_get("canonical_body")?;
            let setup_skill_policy_json: String = skill_row.try_get("policy_json")?;
            let setup_skill_policy_digest: String = skill_row.try_get("policy_digest")?;
            let setup_skill_content_digest: String = skill_row.try_get("content_digest")?;
            let setup_skill_current_revision: Option<String> =
                skill_row.try_get("current_revision_id")?;
            if setup_skill_key != PROJECT_OPERATING_SKILL_KEY
                || setup_skill_revision < 1
                || setup_skill_schema_version != PROJECT_OPERATING_SKILL_SCHEMA_VERSION
                || setup_skill_render_version != PROJECT_OPERATING_SKILL_RENDER_VERSION
                || setup_skill_canonical_body != canonical_project_operating_skill_body()
                || setup_skill_policy_json != PROJECT_OPERATING_SKILL_POLICY_JSON
                || setup_skill_policy_digest != PROJECT_OPERATING_SKILL_POLICY_DIGEST
                || setup_skill_content_digest != PROJECT_OPERATING_SKILL_CONTENT_DIGEST
                || setup_skill_current_revision.as_deref() != Some(setup_skill_revision_id.as_str())
                || setup_skill_revision_id
                    != format!("{PROJECT_OPERATING_SKILL_KEY}@{setup_skill_revision}")
            {
                return Err(ServiceError::invalid_operation(
                    "Project setup operating skill is not the selected server contract",
                ));
            }
            let project_identity_digest = canonical_context_digest(&serde_json::json!({
                "id": &project.id,
                "name": &project.name,
                "paused": project.paused_at.is_some(),
                "charter_status": &project.charter_status,
                "charter_setup_required": project.charter_setup_required,
                "version": project.version,
                "updated_at": &project.updated_at,
            }))?;
            let context_references = vec![
                OperatingContextReference::included(
                    format!("project:{project_id}@v{}", project.version),
                    project_id,
                    "project_identity",
                    format!("v{}", project.version),
                    project_identity_digest,
                    "authenticated_project_identity",
                ),
                OperatingContextReference::included(
                    "project_authority:legacy_unverified",
                    project_id,
                    "project_authority",
                    "legacy_unverified",
                    canonical_context_digest(&serde_json::json!({
                        "authority": "legacy_unverified",
                        "project_id": project_id,
                    }))?,
                    "legacy_setup_restriction",
                ),
                OperatingContextReference::included(
                    "project_setup:charter_adoption_required",
                    project_id,
                    "project_setup",
                    "charter_adoption_required",
                    canonical_context_digest(&serde_json::json!({
                        "setup": "charter_adoption_required",
                        "project_id": project_id,
                    }))?,
                    "legacy_setup_restriction",
                ),
            ];
            let context_reference_displays = context_references
                .iter()
                .map(|reference| reference.display.clone())
                .collect::<Vec<_>>();
            let context = ProjectOperatingSkillContext {
                project_id: project_id.to_owned(),
                binding_id: binding.id.clone(),
                permission_ceiling: PROJECT_SETUP_PERMISSION_CEILING.to_owned(),
                policy_revision: None,
                handoff_payload_hash: None,
                charter_id: None,
                charter_revision: None,
                charter_content_digest: None,
                charter_render_digest: None,
                approval_receipt_id: None,
                project_mode: api_types::ProjectMode::Compact,
                effective_state: EffectiveProjectStateContext::default(),
                context_manifest_references: context_reference_displays,
                profile_text: profile.prompt_template.clone().unwrap_or_default(),
            };
            let mut instruction = render_project_operating_skill(&context);
            instruction.push_str(PROJECT_SETUP_RESTRICTIONS);
            let context_sources = project_operating_context_sources(
                &setup_skill_revision_id,
                &setup_skill_content_digest,
                &context_references,
            );
            return Ok(ProjectOperatingSkillSnapshot {
                instruction,
                context_sources,
            });
        }

        if project.charter_status != "charter_backed"
            || project.charter_setup_required
            || project.current_charter_id.is_none()
            || project.current_charter_revision_id.is_none()
        {
            return Err(ServiceError::invalid_operation(
                "Project Agent turn is blocked until the Project has an approved Charter",
            ));
        }
        let binding_is_executable = if frozen.is_some() {
            matches!(binding_state.as_str(), "active" | "replaced")
        } else {
            binding_state == "active"
        };
        if !binding_is_executable
            || binding_project_id != project_id
            || binding_identity_id.as_deref() != Some(agent.id.as_str())
            || binding_charter_setup_required != 0
            || binding_policy_revision.trim().is_empty()
            || binding_policy_digest.trim().is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "Project Agent binding is not an active Charter-backed authority",
            ));
        }
        let binding_skill_revision_id = binding_skill_revision_id.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent binding has no server-owned operating-skill revision",
            )
        })?;
        let binding_charter_id = binding_charter_id.ok_or_else(|| {
            ServiceError::invalid_operation("Project Agent binding has no Charter pointer")
        })?;
        let binding_charter_revision_id = binding_charter_revision_id.ok_or_else(|| {
            ServiceError::invalid_operation("Project Agent binding has no Charter revision pointer")
        })?;

        let project_charter_id = project.current_charter_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("Project has no current Charter pointer")
        })?;
        let project_charter_revision_id = project
            .current_charter_revision_id
            .as_deref()
            .ok_or_else(|| {
                ServiceError::invalid_operation("Project has no current Charter revision pointer")
            })?;
        if binding_charter_id != project_charter_id
            || binding_charter_revision_id != project_charter_revision_id
        {
            return Err(ServiceError::invalid_operation(
                "Project Agent binding Charter pointers do not match the Project",
            ));
        }

        let charter_row = sqlx::query(
            "SELECT c.id AS charter_id, c.account_id AS charter_account_id,
                    c.genesis_session_id,
                    c.current_approved_revision_id,
                    c.project_mode, c.version AS charter_version,
                    r.id AS charter_revision_id, r.lifecycle AS charter_revision_lifecycle,
                    r.revision AS charter_revision_number,
                    r.schema_version AS charter_schema_version,
                    r.render_version AS charter_render_version,
                    r.content_digest AS charter_content_digest,
                    r.rendered_digest AS charter_render_digest,
                    a.id AS approval_id, a.lifecycle AS approval_lifecycle,
                    a.approval_type,
                    a.content_digest AS approval_content_digest,
                    a.rendered_digest AS approval_render_digest,
                    a.expected_charter_version,
                    a.approved_project_mode,
                    a.approved_name,
                    a.selected_identity_id, a.selected_profile_id,
                    a.selected_operating_skill_revision_id,
                    a.selected_policy_revision, a.selected_policy_digest,
                    a.approval_event_id,
                    a.consumed_project_id,
                    a.approved_slug,
                    a.approving_principal_type,
                    a.approving_principal_id,
                    a.authorization_basis AS approval_authorization_basis,
                    a.authorization_action AS approval_authorization_action,
                    a.explicit_event AS approval_authorization_event_id,
                    a.authorization_occurred_at AS approval_authorization_occurred_at,
                    a.created_at AS approval_created_at
             FROM project_charter AS c
             JOIN project_charter_revision AS r
               ON r.id = c.current_approved_revision_id
              AND r.lifecycle = 'approved'
             JOIN project_charter_approval AS a
               ON a.charter_id = c.id AND a.revision_id = r.id
              AND a.lifecycle IN ('active', 'consumed')
             WHERE c.project_id = ? AND c.id = ? AND r.id = ?
             ORDER BY a.created_at DESC, a.id DESC
             LIMIT 1",
        )
        .bind(project_id)
        .bind(project_charter_id)
        .bind(project_charter_revision_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent turn has no current approved Charter approval receipt",
            )
        })?;
        let charter_id: String = charter_row.try_get("charter_id")?;
        let charter_account_id: String = charter_row.try_get("charter_account_id")?;
        let charter_revision_id: String = charter_row.try_get("charter_revision_id")?;
        let current_approved_revision_id: Option<String> =
            charter_row.try_get("current_approved_revision_id")?;
        let charter_revision_lifecycle: String =
            charter_row.try_get("charter_revision_lifecycle")?;
        let charter_revision_number: i64 = charter_row.try_get("charter_revision_number")?;
        let charter_schema_version: String = charter_row.try_get("charter_schema_version")?;
        let charter_render_version: String = charter_row.try_get("charter_render_version")?;
        let charter_version: i64 = charter_row.try_get("charter_version")?;
        let expected_charter_version: i64 = charter_row.try_get("expected_charter_version")?;
        let approval_lifecycle: String = charter_row.try_get("approval_lifecycle")?;
        let approval_id: String = charter_row.try_get("approval_id")?;
        let approval_type: String = charter_row.try_get("approval_type")?;
        let approval_content_digest: String = charter_row.try_get("approval_content_digest")?;
        let approval_render_digest: String = charter_row.try_get("approval_render_digest")?;
        let approved_project_mode: String = charter_row.try_get("approved_project_mode")?;
        let approved_name: Option<String> = charter_row.try_get("approved_name")?;
        let charter_content_digest: String = charter_row.try_get("charter_content_digest")?;
        let charter_render_digest: String = charter_row.try_get("charter_render_digest")?;
        let consumed_project_id: Option<String> = charter_row.try_get("consumed_project_id")?;
        let selected_identity_id: Option<String> = charter_row.try_get("selected_identity_id")?;
        let selected_profile_id: Option<String> = charter_row.try_get("selected_profile_id")?;
        let selected_skill_revision_id: Option<String> =
            charter_row.try_get("selected_operating_skill_revision_id")?;
        let selected_policy_revision: Option<String> =
            charter_row.try_get("selected_policy_revision")?;
        let selected_policy_digest: Option<String> =
            charter_row.try_get("selected_policy_digest")?;
        let approval_event_id: Option<String> = charter_row.try_get("approval_event_id")?;
        let approval_approved_slug: Option<String> = charter_row.try_get("approved_slug")?;
        let approval_principal_kind: String = charter_row.try_get("approving_principal_type")?;
        let approval_principal_id: String = charter_row.try_get("approving_principal_id")?;
        let approval_authorization_basis: String =
            charter_row.try_get("approval_authorization_basis")?;
        let approval_authorization_action: String =
            charter_row.try_get("approval_authorization_action")?;
        let approval_authorization_event_id: String =
            charter_row.try_get("approval_authorization_event_id")?;
        let approval_authorization_occurred_at: String =
            charter_row.try_get("approval_authorization_occurred_at")?;
        let approval_created_at: String = charter_row.try_get("approval_created_at")?;
        let genesis_session_id: Option<String> = charter_row.try_get("genesis_session_id")?;
        let expected_approval_event_id = approval_event_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent Charter approval has no immutable authorization event",
            )
        })?;
        let expected_identity_id = selected_identity_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent Charter approval has no selected identity",
            )
        })?;
        let expected_profile_revision_id = selected_profile_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent Charter approval has no selected profile revision",
            )
        })?;
        let expected_skill_revision_id =
            selected_skill_revision_id.as_deref().ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project Agent Charter approval has no selected operating skill revision",
                )
            })?;
        let expected_policy_revision = selected_policy_revision.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent Charter approval has no selected policy revision",
            )
        })?;
        let expected_policy_digest = selected_policy_digest.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent Charter approval has no selected policy digest",
            )
        })?;
        let recomputed_policy_digest = project_agent_policy_digest(&profile.tool_policy_json);
        if charter_id != project_charter_id
            || charter_revision_id != project_charter_revision_id
            || current_approved_revision_id.as_deref() != Some(project_charter_revision_id)
            || charter_revision_lifecycle != "approved"
            || approval_lifecycle != "consumed"
            || project.current_charter_version != charter_version
            || expected_charter_version + 1 != project.current_charter_version
            || approval_content_digest != charter_content_digest
            || approval_render_digest != charter_render_digest
            || charter_content_digest.trim().is_empty()
            || charter_render_digest.trim().is_empty()
            || (frozen.is_none() && selected_identity_id.as_deref() != Some(agent.id.as_str()))
            || (frozen.is_none() && selected_profile_id.as_deref() != Some(profile.id.as_str()))
            // The approval receipt immutably records the skill revision that
            // was current at approval time. The skill body is server-owned —
            // the user approved the agent and policy, not the instruction
            // text — so a later server-owned revision of the SAME skill key
            // is admissible; the next block still requires the binding to
            // carry the exact current server contract.
            || !expected_skill_revision_id
                .starts_with(&format!("{PROJECT_OPERATING_SKILL_KEY}@"))
            || (frozen.is_none()
                && selected_policy_revision.as_deref() != Some(binding_policy_revision.as_str()))
            || (frozen.is_none()
                && selected_policy_digest.as_deref() != Some(binding_policy_digest.as_str()))
            || (frozen.is_none() && expected_identity_id != agent.id)
            || (frozen.is_none() && expected_profile_revision_id != profile.id)
            || (frozen.is_none() && expected_policy_revision != binding_policy_revision)
            || (frozen.is_none() && expected_policy_digest != binding_policy_digest)
            || (frozen.is_none() && expected_policy_digest != recomputed_policy_digest)
            || consumed_project_id.as_deref() != Some(project_id)
        {
            return Err(ServiceError::invalid_operation(
                "Project Agent Charter, approval, binding, or selected policy is stale or mismatched",
            ));
        }

        let skill_row = sqlx::query(
            "SELECT sr.id, sr.skill_key, sr.revision,
                    sr.schema_version, sr.render_version, sr.canonical_body,
                    sr.policy_json, sr.policy_digest, sr.content_digest,
                    os.current_revision_id
             FROM operating_skill_revision AS sr
             JOIN operating_skill AS os ON os.id = sr.operating_skill_id
             WHERE sr.id = ?",
        )
        .bind(&binding_skill_revision_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation("Project Agent operating-skill revision is not present")
        })?;
        let skill_id: String = skill_row.try_get("id")?;
        let skill_key: String = skill_row.try_get("skill_key")?;
        let skill_revision: i64 = skill_row.try_get("revision")?;
        let skill_schema_version: String = skill_row.try_get("schema_version")?;
        let skill_render_version: String = skill_row.try_get("render_version")?;
        let skill_canonical_body: String = skill_row.try_get("canonical_body")?;
        let skill_policy_json: String = skill_row.try_get("policy_json")?;
        let skill_policy_digest: String = skill_row.try_get("policy_digest")?;
        let skill_content_digest: String = skill_row.try_get("content_digest")?;
        let historical_skill_valid = frozen.is_some_and(|snapshot| {
            skill_id == snapshot.operating_skill_revision_id
                && skill_key == snapshot.operating_skill_key
                && skill_revision >= 1
                && !skill_schema_version.trim().is_empty()
                && !skill_render_version.trim().is_empty()
                && !skill_canonical_body.trim().is_empty()
                && !skill_policy_json.trim().is_empty()
                && !skill_policy_digest.trim().is_empty()
                && skill_content_digest == snapshot.operating_skill_content_digest
        });
        let current_skill_valid = frozen.is_none()
            && skill_id == binding_skill_revision_id
            && skill_key == PROJECT_OPERATING_SKILL_KEY
            && skill_revision >= 1
            && skill_schema_version == PROJECT_OPERATING_SKILL_SCHEMA_VERSION
            && skill_render_version == PROJECT_OPERATING_SKILL_RENDER_VERSION
            && skill_canonical_body == canonical_project_operating_skill_body()
            && skill_policy_json == PROJECT_OPERATING_SKILL_POLICY_JSON
            && skill_policy_digest == PROJECT_OPERATING_SKILL_POLICY_DIGEST
            && skill_content_digest == PROJECT_OPERATING_SKILL_CONTENT_DIGEST
            && binding_skill_revision_id
                == format!("{PROJECT_OPERATING_SKILL_KEY}@{skill_revision}")
            && skill_row
                .try_get::<Option<String>, _>("current_revision_id")?
                .as_deref()
                == Some(binding_skill_revision_id.as_str());
        if !historical_skill_valid && !current_skill_valid {
            return Err(ServiceError::invalid_operation(
                "Project Agent operating-skill revision is not the selected server contract",
            ));
        }

        let project_owner_id = project.owner_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(
                "Charter-backed Project has no owner for Main-to-Project authentication",
            )
        })?;
        if charter_account_id != project_owner_id {
            return Err(ServiceError::invalid_operation(
                "Project Charter account does not match the Project owner",
            ));
        }
        if agent.owner_id.as_deref() != Some(project_owner_id) {
            return Err(ServiceError::invalid_operation(
                "Project Agent identity is not owned by the Project account",
            ));
        }
        if approval_principal_kind != "user" || approval_principal_id != project_owner_id {
            return Err(ServiceError::invalid_operation(
                "Project Charter approval is not an authenticated user authorization",
            ));
        }
        let approval_event = sqlx::query(
            "SELECT principal_type, principal_id, authorization_basis, action,
                    explicit_event, occurred_at
             FROM project_charter_approval_event
             WHERE approval_id = ? AND id = ? AND principal_type = 'user'
               AND principal_id = ? LIMIT 1",
        )
        .bind(&approval_id)
        .bind(expected_approval_event_id)
        .bind(project_owner_id)
        .fetch_optional(self.db.pool())
        .await?;
        let approval_event = approval_event.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Charter approval event is not an exact user authorization",
            )
        })?;
        if approval_event.try_get::<String, _>("authorization_basis")?
            != approval_authorization_basis
            || approval_event.try_get::<String, _>("action")? != approval_authorization_action
            || approval_event.try_get::<String, _>("explicit_event")?
                != approval_authorization_event_id
            || approval_event.try_get::<String, _>("occurred_at")?
                != approval_authorization_occurred_at
        {
            return Err(ServiceError::invalid_operation(
                "Project Charter approval event is not an exact user authorization",
            ));
        }
        let (handoff_id, handoff_payload_hash) = if genesis_session_id.is_none()
            && matches!(approval_type.as_str(), "adoption" | "charter_amendment")
        {
            let is_adoption = approval_type == "adoption";
            if project.charter_status != "charter_backed"
                || project.charter_setup_required
                || approval_principal_kind != "user"
                || approval_principal_id != project_owner_id
                || approved_name.as_deref() != Some(project.name.as_str())
                || approval_lifecycle != "consumed"
                || consumed_project_id.as_deref() != Some(project_id)
            {
                return Err(ServiceError::invalid_operation(
                    "Project adoption approval is stale or not authenticated",
                ));
            }
            let charter_revision_for_hash = project_charter_revision_id.to_owned();
            let adoption_hash = hash_parts(
                if is_adoption {
                    b"forge-project-adoption-bootstrap-v1\0"
                } else {
                    b"forge-project-charter-amendment-bootstrap-v1\0"
                },
                [&approval_id, &charter_revision_for_hash, &binding.id],
            );
            (None, adoption_hash)
        } else {
            let genesis_session_id = genesis_session_id.ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project Charter has no Product Genesis source for Main provenance",
                )
            })?;
            let genesis_row = sqlx::query(
                "SELECT account_id, main_chat_id, lifecycle, source_message_ids_json
                 FROM product_genesis_session
                 WHERE id = ?",
            )
            .bind(&genesis_session_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project handoff has no Product Genesis source record",
                )
            })?;
            let genesis_account_id: String = genesis_row.try_get("account_id")?;
            let genesis_main_chat_id: String = genesis_row.try_get("main_chat_id")?;
            let genesis_lifecycle: String = genesis_row.try_get("lifecycle")?;
            let genesis_source_message_ids_json: String =
                genesis_row.try_get("source_message_ids_json")?;
            let genesis_source_message_ids: Vec<String> =
                serde_json::from_str(&genesis_source_message_ids_json).map_err(|_| {
                    ServiceError::invalid_operation(
                        "Project handoff Product Genesis source references are invalid",
                    )
                })?;
            if genesis_account_id != project_owner_id
                || genesis_lifecycle != "handed_off"
                || genesis_main_chat_id.trim().is_empty()
            {
                return Err(ServiceError::invalid_operation(
                    "Project handoff Product Genesis/Main provenance is stale or mismatched",
                ));
            }
            let source_turn_id = match genesis_source_message_ids.last() {
                Some(source_message_id) => {
                    sqlx::query_scalar::<_, String>(
                        "SELECT id FROM agent_chat_turn_job
                     WHERE chat_id = ? AND triggering_message_id = ?
                     ORDER BY created_at DESC, id DESC LIMIT 1",
                    )
                    .bind(&genesis_main_chat_id)
                    .bind(source_message_id)
                    .fetch_optional(self.db.pool())
                    .await?
                }
                None => None,
            };
            let handoff_rows = sqlx::query(
                "SELECT handoff.id, handoff.source_chat_id, handoff.target_chat_id,
                        handoff.author_identity_id, handoff.content,
                        handoff.source_revisions_json, handoff.status,
                        handoff.correlation_id, handoff.causation_id,
                        handoff.dedupe_key, handoff.created_at, handoff.updated_at,
                        handoff.target_message_id, handoff.target_turn_job_id,
                        source_chat.kind AS source_kind,
                        source_chat.account_id AS source_account_id,
                        target_chat.kind AS target_kind,
                        target_chat.project_id AS target_project_id
                 FROM agent_handoff AS handoff
                 JOIN agent_chat AS source_chat ON source_chat.id = handoff.source_chat_id
                  AND source_chat.kind = 'account_main'
                  AND source_chat.account_id = ?
                 JOIN agent_chat AS target_chat ON target_chat.id = handoff.target_chat_id
                  AND target_chat.kind = 'project'
                  AND target_chat.project_id = ?
                 JOIN agent_identity AS author ON author.id = handoff.author_identity_id
                  AND author.owner_id = source_chat.account_id
                 WHERE handoff.target_chat_id = ?
                   AND handoff.status = 'delivered'
                   AND json_valid(handoff.source_revisions_json) = 1
                   AND json_extract(handoff.source_revisions_json, '$.schema_version') = ?
                   AND json_extract(handoff.source_revisions_json, '$.approval.id') = ?
                 ORDER BY handoff.created_at DESC, handoff.id DESC",
            )
            .bind(project_owner_id)
            .bind(project_id)
            .bind(project_chat_id)
            .bind(PROJECT_HANDOFF_SCHEMA_VERSION)
            .bind(&approval_id)
            .fetch_all(self.db.pool())
            .await?;
            let mut matching_handoff: Option<(String, String, String)> = None;
            for handoff_row in handoff_rows {
                let source_chat_id: String = handoff_row.try_get("source_chat_id")?;
                let target_chat_id: String = handoff_row.try_get("target_chat_id")?;
                let author_identity_id: Option<String> =
                    handoff_row.try_get("author_identity_id")?;
                let source_kind: String = handoff_row.try_get("source_kind")?;
                let source_account_id: Option<String> = handoff_row.try_get("source_account_id")?;
                let target_kind: String = handoff_row.try_get("target_kind")?;
                let target_project_id: Option<String> = handoff_row.try_get("target_project_id")?;
                let status: String = handoff_row.try_get("status")?;
                if source_kind != "account_main"
                    || source_account_id.as_deref() != Some(project_owner_id)
                    || target_kind != "project"
                    || target_project_id.as_deref() != Some(project_id)
                    || target_chat_id != project_chat_id
                    || source_chat_id == project_chat_id
                    || source_chat_id != genesis_main_chat_id
                    || status != "delivered"
                {
                    continue;
                }
                let handoff_id: String = handoff_row.try_get("id")?;
                let handoff_content: String = handoff_row.try_get("content")?;
                let handoff_source_revisions: String =
                    handoff_row.try_get("source_revisions_json")?;
                let Some(author_identity_id) = author_identity_id.as_deref() else {
                    continue;
                };
                let packet = match parse_project_handoff_packet(&handoff_source_revisions) {
                    Ok(packet) => packet,
                    Err(_) => continue,
                };
                let source_profile_exists: Option<String> = sqlx::query_scalar(
                    "SELECT profile.id
                     FROM agent_profile AS profile
                     JOIN agent_identity AS identity ON identity.id = profile.identity_id
                     WHERE profile.id = ? AND profile.identity_id = ?
                       AND identity.owner_id = ?
                     LIMIT 1",
                )
                .bind(&packet.source.profile_revision_id)
                .bind(author_identity_id)
                .bind(project_owner_id)
                .fetch_optional(self.db.pool())
                .await?;
                if source_profile_exists.is_none() {
                    continue;
                }
                let source_instruction_revision: Option<i64> = sqlx::query_scalar(
                    "SELECT revision FROM agent_chat_instruction_revision
                     WHERE id = ? AND chat_id = ? AND source_type = 'native' AND source_id = ?
                     LIMIT 1",
                )
                .bind(&packet.source.instruction_revision_id)
                .bind(&genesis_main_chat_id)
                .bind(&genesis_session_id)
                .fetch_optional(self.db.pool())
                .await?;
                if source_instruction_revision != Some(packet.source.instruction_revision) {
                    continue;
                }
                if let Some(source_turn_id) = packet.source.turn_id.as_deref() {
                    let source_turn: Option<(Option<String>, Option<String>)> = sqlx::query_as(
                        "SELECT responder_identity_id, profile_id
                             FROM agent_chat_turn_job
                             WHERE id = ? AND chat_id = ? LIMIT 1",
                    )
                    .bind(source_turn_id)
                    .bind(&genesis_main_chat_id)
                    .fetch_optional(self.db.pool())
                    .await?;
                    if source_turn.as_ref().and_then(|turn| turn.0.as_deref())
                        != Some(author_identity_id)
                        || source_turn.as_ref().and_then(|turn| turn.1.as_deref())
                            != Some(packet.source.profile_revision_id.as_str())
                    {
                        continue;
                    }
                }
                let handoff_correlation_id: String = handoff_row.try_get("correlation_id")?;
                let handoff_causation_id: Option<String> = handoff_row.try_get("causation_id")?;
                let handoff_deduplication_key: String = handoff_row.try_get("dedupe_key")?;
                let handoff_created_at: String = handoff_row.try_get("created_at")?;
                let handoff_updated_at: String = handoff_row.try_get("updated_at")?;
                let create_event_payload: String = sqlx::query_scalar(
                    "SELECT payload_json
                     FROM domain_event
                     WHERE dedupe_key = ?
                       AND event_type = 'project.created_from_charter_approval'
                       AND entity_type = 'project'
                       AND entity_id = ?
                     ORDER BY created_at DESC, id DESC LIMIT 1",
                )
                .bind(format!(
                    "project-charter-create:{handoff_deduplication_key}"
                ))
                .bind(project_id)
                .fetch_optional(self.db.pool())
                .await?
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project handoff has no durable Project-creation authorization event",
                    )
                })?;
                let create_event: Value =
                    serde_json::from_str(&create_event_payload).map_err(|_| {
                        ServiceError::invalid_operation(
                            "Project handoff creation authorization event is invalid",
                        )
                    })?;
                let create_authorization = create_event
                    .get("authorization")
                    .and_then(Value::as_object)
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Project handoff creation authorization event is incomplete",
                        )
                    })?;
                let create_authorization_field = |field: &str| -> Result<String> {
                    create_authorization
                        .get(field)
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            ServiceError::invalid_operation(format!(
                                "Project handoff creation authorization is missing {field}"
                            ))
                        })
                };
                let create_authorization_principal_type =
                    create_authorization_field("principal_type")?;
                let create_authorization_principal_id = create_authorization_field("principal_id")?;
                let create_authorization_basis = create_authorization_field("authorization_basis")?;
                let create_authorization_action = create_authorization_field("action")?;
                let create_authorization_event_id = create_authorization_field("event_id")?;
                let create_authorization_occurred_at = create_authorization_field("occurred_at")?;
                let handoff_target_message_id: Option<String> =
                    handoff_row.try_get("target_message_id")?;
                let handoff_target_turn_id: Option<String> =
                    handoff_row.try_get("target_turn_job_id")?;
                let handoff_causation_id = handoff_causation_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project handoff has no immutable causation event",
                    )
                })?;
                let handoff_target_message_id =
                    handoff_target_message_id.as_deref().ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Project handoff has no target message provenance",
                        )
                    })?;
                let handoff_target_turn_id =
                    handoff_target_turn_id.as_deref().ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Project handoff has no target turn provenance",
                        )
                    })?;
                // The handoff records the turn that first consumed it; every
                // later turn in this Project chat re-verifies the same packet
                // against that recorded delivery rather than expecting the
                // handoff to point at itself.
                let handoff_expectation = ProjectHandoffExpectation {
                    handoff_id: &handoff_id,
                    deduplication_key: &handoff_deduplication_key,
                    correlation_id: &handoff_correlation_id,
                    causation_id: handoff_causation_id,
                    source_chat_id: &genesis_main_chat_id,
                    source_identity_id: author_identity_id,
                    source_profile_revision_id: &packet.source.profile_revision_id,
                    source_instruction_revision_id: &packet.source.instruction_revision_id,
                    source_instruction_revision: packet.source.instruction_revision,
                    source_message_ids: genesis_source_message_ids.clone(),
                    source_turn_id: source_turn_id.as_deref(),
                    project_id,
                    project_name: &project.name,
                    project_mode: &approved_project_mode,
                    approved_slug: approval_approved_slug.as_deref(),
                    target_chat_id: project_chat_id,
                    target_binding_id: &binding.id,
                    target_message_id: handoff_target_message_id,
                    target_turn_id: handoff_target_turn_id,
                    charter_id: project_charter_id,
                    charter_revision_id: project_charter_revision_id,
                    charter_revision_number,
                    charter_schema_version: &charter_schema_version,
                    charter_content_digest: &charter_content_digest,
                    charter_render_version: &charter_render_version,
                    charter_render_digest: &charter_render_digest,
                    approval_id: &approval_id,
                    approval_event_id: expected_approval_event_id,
                    approval_authorization_basis: &approval_authorization_basis,
                    approval_authorization_action: &approval_authorization_action,
                    approval_authorization_event_id: &approval_authorization_event_id,
                    approval_authorization_occurred_at: &approval_authorization_occurred_at,
                    approval_principal_kind: &approval_principal_kind,
                    approval_principal_id: project_owner_id,
                    approval_created_at: &approval_created_at,
                    create_authorization_principal_type: &create_authorization_principal_type,
                    create_authorization_principal_id: &create_authorization_principal_id,
                    create_authorization_basis: &create_authorization_basis,
                    create_authorization_action: &create_authorization_action,
                    create_authorization_event_id: &create_authorization_event_id,
                    create_authorization_occurred_at: &create_authorization_occurred_at,
                    identity_id: &agent.id,
                    profile_revision_id: &profile.id,
                    policy_revision: &binding_policy_revision,
                    policy_digest: &recomputed_policy_digest,
                    created_at: &handoff_created_at,
                    delivered_at: &handoff_updated_at,
                };
                if validate_project_handoff_packet(
                    &handoff_source_revisions,
                    &handoff_expectation,
                    &approval_lifecycle,
                    consumed_project_id.as_deref(),
                    project_id,
                )
                .is_err()
                {
                    continue;
                }
                if matching_handoff.is_some() {
                    return Err(ServiceError::invalid_operation(
                        "Project Agent turn has multiple handoffs for the exact Charter approval",
                    ));
                }
                matching_handoff = Some((handoff_id, handoff_content, handoff_source_revisions));
            }
            let (handoff_id, handoff_content, handoff_source_revisions) = matching_handoff
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project Agent turn has no exact consumed Charter handoff",
                    )
                })?;
            let handoff_payload_hash = hash_parts(
                b"forge-project-handoff-payload-v1\0",
                [&handoff_id, &handoff_content, &handoff_source_revisions],
            );
            (Some(handoff_id), handoff_payload_hash)
        };

        let project_mode = match charter_row.try_get::<String, _>("project_mode")?.as_str() {
            "compact" => api_types::ProjectMode::Compact,
            "standard" => api_types::ProjectMode::Standard,
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Project Charter has an unsupported Project mode",
                ));
            }
        };

        let project_identity_digest = canonical_context_digest(&serde_json::json!({
            "id": &project.id,
            "name": &project.name,
            "paused": project.paused_at.is_some(),
            "charter_status": &project.charter_status,
            "charter_setup_required": project.charter_setup_required,
            "version": project.version,
            "updated_at": &project.updated_at,
            "charter_id": &charter_id,
            "charter_revision_id": &charter_revision_id,
            "charter_content_digest": &charter_content_digest,
            "charter_render_digest": &charter_render_digest,
        }))?;
        let approval_receipt_digest = canonical_context_digest(&serde_json::json!({
            "id": &approval_id,
            "event_id": expected_approval_event_id,
            "principal_type": &approval_principal_kind,
            "principal_id": &approval_principal_id,
            "authorization_basis": &approval_authorization_basis,
            "authorization_action": &approval_authorization_action,
            "authorization_occurred_at": &approval_authorization_occurred_at,
            "created_at": &approval_created_at,
            "charter_id": &charter_id,
            "revision_id": &charter_revision_id,
            "content_digest": &charter_content_digest,
            "render_digest": &charter_render_digest,
        }))?;

        let mut context_references = vec![
            OperatingContextReference::included(
                format!("project:{project_id}@v{}", project.version),
                project_id,
                "project_identity",
                format!("v{}", project.version),
                project_identity_digest,
                "authenticated_project_identity",
            ),
            OperatingContextReference::included(
                format!(
                    "binding:{}@policy:{}:digest:{}",
                    binding.id, binding_policy_revision, binding_policy_digest
                ),
                &binding.id,
                "project_agent_binding",
                binding_policy_revision.clone(),
                &binding_policy_digest,
                "authenticated_project_binding_policy",
            ),
            OperatingContextReference::included(
                format!(
                    "operating_skill:{}:content:{}",
                    binding_skill_revision_id, skill_content_digest
                ),
                &binding_skill_revision_id,
                "server_operating_skill",
                binding_skill_revision_id.clone(),
                &skill_content_digest,
                "canonical_project_operating_skill",
            ),
            OperatingContextReference::included(
                format!(
                    "charter:{charter_id}@{charter_revision_id}:content:{charter_content_digest}:render:{charter_render_digest}"
                ),
                &charter_id,
                "project_charter",
                charter_revision_id.clone(),
                &charter_content_digest,
                "current_approved_project_charter",
            ),
            OperatingContextReference::included(
                format!(
                    "approval:{approval_id}@event:{}",
                    expected_approval_event_id
                ),
                &approval_id,
                "charter_approval_receipt",
                expected_approval_event_id.to_owned(),
                approval_receipt_digest,
                "consumed_project_charter_approval",
            ),
        ];
        if let Some(handoff_id) = handoff_id.as_deref() {
            context_references.push(OperatingContextReference::included(
                format!("handoff:{handoff_id}:receipt:{handoff_payload_hash}"),
                handoff_id,
                "main_to_project_handoff_receipt",
                "forge.project-charter-handoff/v1",
                &handoff_payload_hash,
                "opaque_handoff_provenance_only",
            ));
        } else if approval_type == "adoption" {
            context_references.push(OperatingContextReference::included(
                format!("adoption_bootstrap:{approval_id}:receipt:{handoff_payload_hash}"),
                &approval_id,
                "project_adoption_bootstrap",
                charter_revision_id.clone(),
                &handoff_payload_hash,
                "consumed_user_adoption_approval",
            ));
        } else {
            context_references.push(OperatingContextReference::included(
                format!("charter_amendment_bootstrap:{approval_id}:receipt:{handoff_payload_hash}"),
                &approval_id,
                "charter_amendment_bootstrap",
                charter_revision_id.clone(),
                &handoff_payload_hash,
                "consumed_user_charter_amendment_approval",
            ));
        }

        let projection = load_effective_project_state(&self.db, project_id, Some(32)).await?;
        if projection
            .governing_charter
            .as_ref()
            .map(|charter| format!("{}@{}", charter.id, charter.revision_id))
            .as_deref()
            != Some(&format!("{charter_id}@{charter_revision_id}"))
        {
            return Err(ServiceError::invalid_operation(
                "Project effective-state projection does not match the authenticated Charter",
            ));
        }
        let effective_state = effective_state_context(&projection)?;

        if let Some(baseline) = projection.active_execution_baseline.as_ref() {
            context_references.push(OperatingContextReference::included(
                format!(
                    "baseline:{}@{}:content:{}:render:{}:policy:{}@{}",
                    baseline.id,
                    baseline.revision_id,
                    baseline.content_digest,
                    baseline.render_digest,
                    baseline.release_policy_revision,
                    baseline.release_policy_digest
                ),
                &baseline.id,
                "execution_baseline",
                &baseline.revision_id,
                &baseline.content_digest,
                "active_approved_execution_baseline",
            ));
        } else {
            context_references.push(OperatingContextReference::omitted(
                "baseline:unresolved",
                project_id,
                "execution_baseline",
                "unresolved",
                canonical_context_digest(&serde_json::json!({
                    "project_id": project_id,
                    "source_event_watermark": &projection.source_event_watermark,
                    "execution_baseline": Value::Null,
                }))?,
                "no_active_approved_execution_baseline",
            ));
        }

        for document in &projection.approved_documents {
            context_references.push(OperatingContextReference::included(
                format!(
                    "document:{}:{}@{}:content:{}:render:{}",
                    document.kind,
                    document.id,
                    document.revision_id,
                    document.content_digest,
                    document.render_digest
                ),
                &document.id,
                "project_document",
                &document.revision_id,
                &document.content_digest,
                "current_approved_document",
            ));
        }

        for decision in &projection.active_decisions {
            let digest = canonical_context_digest(decision)?;
            context_references.push(OperatingContextReference::included(
                format!(
                    "decision:{}@{}:digest:{}",
                    decision.id, decision.created_at, digest
                ),
                &decision.id,
                "project_decision",
                &decision.created_at,
                digest,
                "active_effective_decision",
            ));
        }
        for decision in &projection.invalidated_decisions {
            let digest = canonical_context_digest(decision)?;
            context_references.push(OperatingContextReference::omitted(
                format!("decision:{}@stale:digest:{}", decision.id, digest),
                &decision.id,
                "project_decision",
                &decision.created_at,
                digest,
                "invalidated_decision",
            ));
        }

        for reconciliation in &projection.reconciliation_required {
            context_references.push(OperatingContextReference::omitted(
                format!(
                    "reconciliation:{}:{}:{}@{}:digest:{}",
                    reconciliation.id,
                    reconciliation.record_type,
                    reconciliation.record_id,
                    reconciliation.record_revision,
                    reconciliation.record_digest
                ),
                &reconciliation.record_id,
                "project_reconciliation",
                &reconciliation.record_revision,
                &reconciliation.record_digest,
                "reconciliation_required",
            ));
        }
        for conflict in &projection.canonical_conflicts {
            context_references.push(OperatingContextReference::omitted(
                format!(
                    "conflict:{}:{}:{}@{}:{}@{}",
                    conflict.id,
                    conflict.domain,
                    conflict.governing_record_id,
                    conflict.governing_record_revision,
                    conflict.conflicting_record_id,
                    conflict.conflicting_record_revision
                ),
                &conflict.id,
                "canonical_project_conflict",
                &conflict.created_at,
                &conflict.governing_record_digest,
                "canonical_conflict_blocks_affected_work",
            ));
        }

        let task_summary = projection
            .task_summary
            .by_status
            .iter()
            .map(|count| format!("{}={}", count.key, count.count))
            .collect::<Vec<_>>()
            .join(",");
        let task_summary_digest = canonical_context_digest(&projection.task_summary)?;
        context_references.push(OperatingContextReference::included(
            format!(
                "tasks:summary:{}:total={}@{}",
                task_summary, projection.task_summary.total, task_summary_digest
            ),
            project_id,
            "project_task_projection",
            &projection.source_event_watermark,
            task_summary_digest,
            "latest_server_accepted_task_versions",
        ));

        let validation_summary = projection
            .validation_summary
            .by_outcome
            .iter()
            .map(|count| format!("{}={}", count.key, count.count))
            .collect::<Vec<_>>()
            .join(",");
        let validation_summary_digest = canonical_context_digest(&projection.validation_summary)?;
        context_references.push(OperatingContextReference::included(
            format!(
                "validation:summary:{}:total={}@{}",
                validation_summary, projection.validation_summary.total, validation_summary_digest
            ),
            project_id,
            "project_validation_projection",
            &projection.source_event_watermark,
            validation_summary_digest,
            "latest_validation_summary",
        ));

        // Coordination state is a bounded reconciliation projection.  Only
        // server-derived ids, lifecycle/version metadata, and evidence counts
        // enter the Project prompt; inbox bodies and commitment descriptions
        // remain behind explicit scoped read tools and cannot inject prompt
        // instructions.
        for commitment in &projection.commitments {
            let digest = canonical_context_digest(commitment)?;
            context_references.push(OperatingContextReference::included(
                format!(
                    "commitment:{}:{}@v{}:evidence:{}",
                    commitment.id, commitment.status, commitment.version, commitment.evidence_count
                ),
                &commitment.id,
                "project_commitment",
                format!("v{}", commitment.version),
                digest,
                "project_commitment_reconciliation",
            ));
        }
        for item in &projection.inbox {
            let digest = canonical_context_digest(item)?;
            context_references.push(OperatingContextReference::included(
                format!(
                    "inbox:{}:{}:{}@v{}",
                    item.id, item.kind, item.status, item.version
                ),
                &item.id,
                "project_inbox_reconciliation",
                format!("v{}", item.version),
                digest,
                "project_inbox_reconciliation",
            ));
        }

        for milestone in &projection.active_milestones {
            context_references.push(
                match (
                    milestone.definition_revision_id.as_deref(),
                    milestone.definition_digest.as_deref(),
                ) {
                    (Some(definition_revision), Some(definition_digest))
                        if !definition_revision.trim().is_empty()
                            && !definition_digest.trim().is_empty() =>
                    {
                        OperatingContextReference::included(
                            format!(
                                "milestone:{}@{}:content:{}",
                                milestone.id, definition_revision, definition_digest
                            ),
                            &milestone.id,
                            "project_milestone_definition",
                            definition_revision,
                            definition_digest,
                            "current_project_milestone_definition",
                        )
                    }
                    _ => OperatingContextReference::omitted(
                        format!("milestone:{}@stale:missing_definition", milestone.id),
                        &milestone.id,
                        "project_milestone_definition",
                        "unresolved",
                        canonical_context_digest(milestone)?,
                        "missing_milestone_definition",
                    ),
                },
            );
        }

        if let Some(readiness) = projection.readiness.latest.as_ref() {
            context_references.push(OperatingContextReference::included(
                format!(
                    "readiness:{}@{}:{}:digest:{}",
                    readiness.id,
                    readiness.event_watermark,
                    readiness.outcome,
                    readiness.readiness_digest
                ),
                &readiness.id,
                "project_readiness_snapshot",
                &readiness.id,
                &readiness.readiness_digest,
                "latest_readiness_snapshot",
            ));
        } else {
            context_references.push(OperatingContextReference::omitted(
                "readiness:unresolved",
                project_id,
                "project_readiness_snapshot",
                &projection.source_event_watermark,
                canonical_context_digest(&serde_json::json!({
                    "project_id": project_id,
                    "source_event_watermark": &projection.source_event_watermark,
                    "readiness": Value::Null,
                }))?,
                "no_readiness_snapshot",
            ));
        }

        for release in &projection.releases {
            let release_revision = release.release_revision.to_string();
            context_references.push(OperatingContextReference::included(
                format!(
                    "release:{}:{}@{}:readiness:{}:snapshot:{}:baseline:{}@{}",
                    release.id,
                    release.release_identifier,
                    release.release_revision,
                    release.readiness_digest,
                    release.snapshot_digest,
                    release.baseline_id,
                    release.baseline_revision_id
                ),
                &release.id,
                "immutable_project_release",
                &release_revision,
                &release.snapshot_digest,
                "immutable_release_history",
            ));
        }

        let unreleased = &projection.unreleased_changes;
        for (source_type, ids, reason) in [
            (
                "unreleased_document_change",
                &unreleased.document_ids,
                "current_document_draft_differs_from_approved",
            ),
            (
                "unreleased_decision_candidate",
                &unreleased.decision_candidate_ids,
                "decision_candidate_not_effective",
            ),
            (
                "unreleased_baseline_revision",
                &unreleased.baseline_revision_ids,
                "baseline_revision_not_active",
            ),
            (
                "unreleased_active_milestone",
                &unreleased.active_milestone_ids,
                "active_milestone_has_no_release",
            ),
            (
                "unreleased_reconciliation",
                &unreleased.reconciliation_ids,
                "reconciliation_record_remains_required",
            ),
        ] {
            for id in ids {
                let digest = canonical_context_digest(&serde_json::json!({
                    "source_type": source_type,
                    "source_id": id,
                    "source_event_watermark": &projection.source_event_watermark,
                }))?;
                context_references.push(OperatingContextReference::omitted(
                    format!("{source_type}:{id}:digest:{digest}"),
                    id,
                    source_type,
                    &projection.source_event_watermark,
                    digest,
                    reason,
                ));
            }
        }

        // The prompt and manifest use the same canonical projection.  Raw
        // handoff packet JSON, Main chat IDs, and Main transcript bodies never
        // enter these references.

        let context = ProjectOperatingSkillContext {
            project_id: project_id.to_owned(),
            binding_id: binding.id,
            permission_ceiling: binding.permission_ceiling_json,
            policy_revision: Some(binding_policy_revision.clone()),
            handoff_payload_hash: handoff_id.as_ref().map(|_| handoff_payload_hash),
            charter_id: Some(charter_id),
            charter_revision: Some(charter_revision_id),
            charter_content_digest: Some(charter_content_digest),
            charter_render_digest: Some(charter_render_digest),
            approval_receipt_id: Some(approval_id),
            project_mode,
            effective_state,
            context_manifest_references: context_references
                .iter()
                .map(|reference| reference.display.clone())
                .collect(),
            profile_text: profile.prompt_template.clone().unwrap_or_default(),
        };
        let instruction = replace_rendered_skill_body(
            render_project_operating_skill(&context),
            frozen.map(|snapshot| snapshot.operating_skill_body.as_str()),
            canonical_project_operating_skill_body(),
        )?;
        let context_sources = project_operating_context_sources(
            &binding_skill_revision_id,
            &skill_content_digest,
            &context_references,
        );
        Ok(ProjectOperatingSkillSnapshot {
            instruction,
            context_sources,
        })
    }

    async fn run_native(
        &self,
        job: &AgentChatTurnJob,
        turn: LoadedAgentChatTurn,
        cancellation: CancellationToken,
    ) -> Result<CompletedAgentChatTurn> {
        let LoadedAgentChatTurn {
            agent,
            profile,
            session,
            input,
            history,
            operating_instruction,
            operating_context_sources,
        } = turn;
        let owner_user_id = agent
            .owner_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent identity has no owner"))?;
        let credential_ref = profile
            .credential_ref
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent profile has no credential"))?;
        let config: NativeProfileConfig = serde_json::from_str(&profile.config_json)
            .map_err(|_| ServiceError::invalid_operation("Agent profile config is invalid"))?;
        let runtime_session_id = session
            .runtime_session_id
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("Agent session has no runtime id"))?;
        let provider = profile
            .provider
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("Agent profile has no provider"))?;
        let model = profile
            .model
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("Agent profile has no model"))?;
        let provider_account_id =
            CredentialHandleRepo::get_credential_handle(&*self.db, credential_ref)
                .await?
                .as_ref()
                .and_then(crate::embedded_agent_service::entry_provider_account_id);
        let started = std::time::Instant::now();
        let output = self
            .embedded_agents
            .native_backend()
            .run_turn(
                AgentTurnRequest {
                    forge_session_id: session.id.clone(),
                    runtime_session_id,
                    scope: CanonicalScope {
                        scope_type: CanonicalScopeType::AgentChat,
                        scope_id: job.chat_id.clone(),
                        workspace_access: WorkspaceAccess::Deny,
                    },
                    workspace_path: None,
                    provider: {
                        let (context_tokens, max_input_tokens, max_output_tokens) =
                            crate::embedded_agent_service::effective_native_limits(
                                &provider,
                                config.context_tokens,
                                config.max_input_tokens,
                                config.max_output_tokens,
                            );
                        NativeProviderConfig {
                            provider,
                            base_url: config.base_url,
                            model: model.clone(),
                            credential_handle_id: credential_ref.to_owned(),
                            owner_user_id: owner_user_id.to_owned(),
                            provider_account_id,
                            context_tokens,
                            max_input_tokens,
                            max_output_tokens,
                        }
                    },
                    system_prompt: compose_system_prompt(
                        profile.prompt_template.as_deref(),
                        operating_instruction.as_deref(),
                    ),
                    history: runtime_history(&history),
                    input: input.content,
                    cancellation,
                },
                Arc::new(NoopTurnEventSink),
            )
            .await
            .map_err(|error| {
                tracing::warn!(
                    job_id = %job.id,
                    chat_id = %job.chat_id,
                    %error,
                    "native Agent Chat turn failed"
                );
                ServiceError::invalid_operation(format!("native Agent Chat turn failed: {error}"))
            })?;
        let content = output.text.trim().to_owned();
        guard_agent_chat_content(&content)?;
        let context_manifest_id = if let Some(manifest) = output.context_manifest.as_ref() {
            Some(
                self.persist_runtime_context_manifest(
                    job,
                    &agent,
                    &profile,
                    &session,
                    Some(&model),
                    manifest,
                    &operating_context_sources,
                )
                .await?,
            )
        } else if !operating_context_sources.is_empty() {
            Some(
                self.persist_server_context_manifest(
                    job,
                    &agent,
                    &profile,
                    &session,
                    Some(&model),
                    &operating_context_sources,
                )
                .await?,
            )
        } else {
            None
        };
        Ok(CompletedAgentChatTurn {
            identity_id: agent.id,
            profile_id: profile.id,
            session_id: session.id,
            model: Some(model),
            content,
            token_usage_json: Some(
                serde_json::json!({
                    "input": output.input_tokens,
                    "output": output.output_tokens,
                })
                .to_string(),
            ),
            duration_ms: started.elapsed().as_millis() as i64,
            context_manifest_id,
            pending_interaction_id: output.pending_interaction_id,
        })
    }

    /// Persist only the final runtime manifest's redaction-safe linkage. The
    /// runtime remains the owner of context ordering and bodies; Forge stores
    /// identifiers, revisions, counts and fingerprints and links the result
    /// to the canonical Agent Chat session before the response is admitted.
    #[allow(clippy::too_many_arguments)]
    async fn persist_runtime_context_manifest(
        &self,
        job: &AgentChatTurnJob,
        agent: &Agent,
        profile: &AgentProfile,
        session: &AgentSession,
        model: Option<&str>,
        runtime_manifest: &RuntimeContextManifestLink,
        operating_context_sources: &[ContextSourceInput],
    ) -> Result<String> {
        let identity_id = uuid::Uuid::parse_str(&agent.id)
            .map_err(|_| ServiceError::invalid_operation("Agent identity id is invalid"))?;
        let context_scope_id = uuid::Uuid::parse_str(&session.context_scope_id).map_err(|_| {
            ServiceError::invalid_operation("Agent Chat context scope id is invalid")
        })?;
        let manifest_id = agent_chat_manifest_id(&agent.id, &session.id, runtime_manifest);
        let request_fingerprint = agent_chat_request_fingerprint(
            job,
            profile,
            session,
            model,
            runtime_manifest,
            operating_context_sources,
        )?;
        let runtime_sources = runtime_manifest_sources(runtime_manifest);
        let mut sources = operating_context_sources.to_vec();
        let source_offset = sources.len() as i64;
        for (offset, mut source) in runtime_sources.into_iter().enumerate() {
            source.ordinal = source_offset + offset as i64;
            sources.push(source);
        }
        let service = ContextManifestService::new(Arc::clone(&self.db));

        if let Some(existing) = service
            .get_authorized(manifest_id, identity_id, context_scope_id)
            .await?
        {
            if existing.runtime_manifest_fingerprint.as_deref()
                != Some(runtime_manifest.runtime_manifest_fingerprint.as_str())
                || existing.request_fingerprint != request_fingerprint
            {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat runtime context manifest idempotency conflict",
                ));
            }
            let existing_sources = service
                .sources(manifest_id, identity_id, context_scope_id)
                .await?;
            for source in &sources {
                if existing_sources.iter().any(|stored| {
                    stored.ordinal == source.ordinal
                        && stored.source_id == source.source_id
                        && stored.source_revision == source.source_revision
                }) {
                    continue;
                }
                service
                    .append_source(manifest_id, identity_id, context_scope_id, source.clone())
                    .await?;
            }
            return Ok(manifest_id.to_string());
        }

        let created = service
            .create(
                ContextManifestInput {
                    id: manifest_id,
                    identity_id,
                    agent_session_id: Some(uuid::Uuid::parse_str(&session.id).map_err(|_| {
                        ServiceError::invalid_operation("Agent Chat session id is invalid")
                    })?),
                    context_scope_id,
                    scope_type: "agent_chat".to_owned(),
                    scope_id: job.chat_id.clone(),
                    policy_revision: "forge-agent-chat-context-policy-1".to_owned(),
                    domain_revision: "forge-agent-chat-runtime-link-1".to_owned(),
                    lcm_binding_revision: runtime_manifest.lcm_binding_revision.clone(),
                    runtime_manifest_id: Some(runtime_manifest.turn_id.clone()),
                    runtime_manifest_fingerprint: Some(
                        runtime_manifest.runtime_manifest_fingerprint.clone(),
                    ),
                    request_fingerprint,
                },
                &sources,
            )
            .await?;
        let created_id = uuid::Uuid::parse_str(&created.id).map_err(|_| {
            ServiceError::invalid_operation("persisted context manifest id is invalid")
        })?;
        for source in sources {
            service
                .append_source(created_id, identity_id, context_scope_id, source)
                .await?;
        }
        Ok(created.id)
    }

    /// Persist the server-owned context even when the selected backend does
    /// not expose an Agent Runtime manifest (for example, the CLI backend).
    ///
    /// Main/Project operating sources are already bounded, redaction-safe
    /// references produced by `load_turn`.  They must not disappear merely
    /// because the provider adapter has no runtime manifest to attach.  The
    /// durable turn job is the idempotency boundary here: a retry of the same
    /// job reuses the same immutable manifest, while a changed source/profile
    /// set is rejected as an idempotency conflict.
    async fn persist_server_context_manifest(
        &self,
        job: &AgentChatTurnJob,
        agent: &Agent,
        profile: &AgentProfile,
        session: &AgentSession,
        model: Option<&str>,
        operating_context_sources: &[ContextSourceInput],
    ) -> Result<String> {
        let identity_id = uuid::Uuid::parse_str(&agent.id)
            .map_err(|_| ServiceError::invalid_operation("Agent identity id is invalid"))?;
        let session_id = uuid::Uuid::parse_str(&session.id)
            .map_err(|_| ServiceError::invalid_operation("Agent Chat session id is invalid"))?;
        let context_scope_id = uuid::Uuid::parse_str(&session.context_scope_id).map_err(|_| {
            ServiceError::invalid_operation("Agent Chat context scope id is invalid")
        })?;
        let manifest_id = agent_chat_server_manifest_id(&agent.id, &session.id, &job.id);
        let request_fingerprint = agent_chat_server_request_fingerprint(
            job,
            profile,
            session,
            model,
            operating_context_sources,
        )?;
        let service = ContextManifestService::new(Arc::clone(&self.db));

        if let Some(existing) = service
            .get_authorized(manifest_id, identity_id, context_scope_id)
            .await?
        {
            if existing.request_fingerprint != request_fingerprint {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat server context manifest idempotency conflict",
                ));
            }
            let existing_sources = service
                .sources(manifest_id, identity_id, context_scope_id)
                .await?;
            for source in operating_context_sources {
                if existing_sources.iter().any(|stored| {
                    stored.ordinal == source.ordinal
                        && stored.source_id == source.source_id
                        && stored.source_revision == source.source_revision
                }) {
                    continue;
                }
                service
                    .append_source(manifest_id, identity_id, context_scope_id, source.clone())
                    .await?;
            }
            return Ok(manifest_id.to_string());
        }

        let created = service
            .create(
                ContextManifestInput {
                    id: manifest_id,
                    identity_id,
                    agent_session_id: Some(session_id),
                    context_scope_id,
                    scope_type: "agent_chat".to_owned(),
                    scope_id: job.chat_id.clone(),
                    policy_revision: "forge-agent-chat-context-policy-1".to_owned(),
                    domain_revision: "forge-agent-chat-server-context-1".to_owned(),
                    lcm_binding_revision: None,
                    runtime_manifest_id: None,
                    runtime_manifest_fingerprint: None,
                    request_fingerprint,
                },
                operating_context_sources,
            )
            .await?;
        let created_id = uuid::Uuid::parse_str(&created.id).map_err(|_| {
            ServiceError::invalid_operation("persisted context manifest id is invalid")
        })?;
        for source in operating_context_sources {
            service
                .append_source(created_id, identity_id, context_scope_id, source.clone())
                .await?;
        }
        Ok(created.id)
    }

    async fn run_cli(
        &self,
        job: &AgentChatTurnJob,
        turn: LoadedAgentChatTurn,
        cancellation: CancellationToken,
    ) -> Result<CompletedAgentChatTurn> {
        let LoadedAgentChatTurn {
            agent,
            profile,
            session,
            input,
            history,
            operating_instruction,
            operating_context_sources,
        } = turn;
        let prompt = build_cli_prompt(
            profile.prompt_template.as_deref(),
            operating_instruction.as_deref(),
            &history,
            &input.content,
        );
        let config = cli_profile_execution_config(&profile)?;
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: job.chat_id.clone(),
            workspace_access: WorkspaceAccess::Deny,
        };
        let (result, duration_ms) = self
            .cli_backend
            .run_turn(
                &scope,
                &job.id,
                &job.chat_id,
                &profile.executor_type,
                config,
                prompt,
                cancellation,
            )
            .await?;
        let content = cli_result_content(result)?;
        guard_agent_chat_content(&content)?;
        let context_manifest_id = if operating_context_sources.is_empty() {
            None
        } else {
            Some(
                self.persist_server_context_manifest(
                    job,
                    &agent,
                    &profile,
                    &session,
                    profile.model.as_deref(),
                    &operating_context_sources,
                )
                .await?,
            )
        };
        Ok(CompletedAgentChatTurn {
            identity_id: agent.id,
            profile_id: profile.id,
            session_id: session.id,
            model: profile.model,
            content,
            token_usage_json: None,
            duration_ms,
            context_manifest_id,
            pending_interaction_id: None,
        })
    }
}

#[async_trait]
impl AgentChatTurnRunner for FederatedAgentChatTurnRunner {
    async fn run_turn(
        &self,
        job: &AgentChatTurnJob,
        cancellation: CancellationToken,
    ) -> Result<CompletedAgentChatTurn> {
        let turn = self.load_turn(job).await?;
        let backend_kind = turn.profile.backend_kind.clone();
        match backend_kind.as_str() {
            "native" => self.run_native(job, turn, cancellation).await,
            "cli" => self.run_cli(job, turn, cancellation).await,
            _ => Err(ServiceError::invalid_operation(
                "selected Agent Chat backend is unsupported",
            )),
        }
    }
}

#[derive(Clone)]
pub struct AgentChatTurnWorker {
    db: Arc<SqliteDb>,
    chat_service: Arc<AgentChatService<SqliteDb>>,
    runner: Arc<dyn AgentChatTurnRunner>,
    lease_owner: String,
}

#[derive(Debug, Clone)]
struct GenesisControlTransfer {
    command_receipt_id: String,
    continuation_turn_id: String,
    genesis_session_id: String,
}

impl fmt::Debug for AgentChatTurnWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentChatTurnWorker")
            .field("lease_owner", &self.lease_owner)
            .finish_non_exhaustive()
    }
}

impl AgentChatTurnWorker {
    pub fn new(
        db: Arc<SqliteDb>,
        embedded_agents: Arc<EmbeddedAgentService>,
        cli_executor: Arc<dyn TaskExecutor>,
    ) -> Self {
        let runner = Arc::new(FederatedAgentChatTurnRunner::new(
            Arc::clone(&db),
            embedded_agents,
            cli_executor,
        ));
        Self::with_runner(db, runner)
    }

    pub fn with_runner(db: Arc<SqliteDb>, runner: Arc<dyn AgentChatTurnRunner>) -> Self {
        Self {
            chat_service: Arc::new(AgentChatService::new(Arc::clone(&db))),
            db,
            runner,
            lease_owner: format!("agent-chat-worker:{}", db::new_uuid_v4()),
        }
    }

    pub fn start(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let cancellation = CancellationToken::new();
            let mut active = tokio::task::JoinSet::new();
            let mut poll = tokio::time::interval(POLL_INTERVAL);
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow_and_update() { break; }
                    }
                    _ = poll.tick(), if active.len() < MAX_ACTIVE_TURNS => {
                        match self.claim_available(MAX_ACTIVE_TURNS - active.len()).await {
                            Ok(jobs) => for job in jobs {
                                let worker = Arc::clone(&self);
                                let token = cancellation.child_token();
                                active.spawn(async move { worker.process_claimed(job, token).await; });
                            },
                            Err(error) => tracing::warn!(error = %error, "Agent Chat turn polling failed"),
                        }
                    }
                    Some(result) = active.join_next(), if !active.is_empty() => {
                        if let Err(error) = result {
                            tracing::warn!(error = %error, "Agent Chat worker task stopped unexpectedly");
                        }
                    }
                }
            }
            cancellation.cancel();
            while let Some(result) = active.join_next().await {
                if let Err(error) = result {
                    tracing::warn!(error = %error, "Agent Chat worker task stopped during shutdown");
                }
            }
        })
    }

    pub async fn run_once(&self) -> Result<usize> {
        self.recover_expired().await?;
        let jobs = self.claim_available(1).await?;
        let count = jobs.len();
        for job in jobs {
            self.process_claimed(job, CancellationToken::new()).await;
        }
        Ok(count)
    }

    async fn claim_available(&self, capacity: usize) -> Result<Vec<AgentChatTurnJob>> {
        let mut jobs = Vec::with_capacity(capacity);
        self.recover_expired().await?;
        for _ in 0..capacity {
            let Some(job) = self.claim_one().await? else {
                break;
            };
            jobs.push(job);
        }
        Ok(jobs)
    }

    async fn claim_one(&self) -> Result<Option<AgentChatTurnJob>> {
        let now = now_rfc3339();
        let leased_until = lease_deadline();
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let id = sqlx::query_scalar::<_, String>(
            "WITH candidate AS (
                 SELECT job.id
                 FROM agent_chat_turn_job AS job
                 WHERE job.status IN ('queued', 'retry_wait')
                   AND job.attempt_count < job.max_attempts
                   AND (job.next_attempt_at IS NULL OR job.next_attempt_at <= ?)
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_chat_turn_job AS prior
                       WHERE prior.id <> job.id
                         AND prior.responder_identity_id = job.responder_identity_id
                         AND prior.canonical_scope_id = job.canonical_scope_id
                         AND prior.status IN ('queued', 'leased', 'retry_wait', 'awaiting_input')
                         AND (prior.created_at < job.created_at
                              OR (prior.created_at = job.created_at AND prior.id < job.id))
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_chat_turn_job AS active
                       WHERE active.chat_id = job.chat_id AND active.status = 'leased'
                   )
                 ORDER BY job.created_at ASC, job.id ASC
                 LIMIT 1
             )
             UPDATE agent_chat_turn_job
             SET status = 'leased', lease_owner = ?, leased_until = ?,
                 attempt_count = attempt_count + 1, next_attempt_at = NULL,
                 version = version + 1, updated_at = ?
             WHERE id = (SELECT id FROM candidate)
               AND status IN ('queued', 'retry_wait')
             RETURNING id",
        )
        .bind(&now)
        .bind(&self.lease_owner)
        .bind(&leased_until)
        .bind(&now)
        .fetch_optional(&mut *transaction)
        .await?;
        transaction.commit().await?;
        let Some(id) = id else { return Ok(None) };
        AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat_turn_job", id))
            .map(Some)
    }

    async fn recover_expired(&self) -> Result<()> {
        let now = now_rfc3339();
        let expired = sqlx::query(
            "SELECT id, attempt_count, max_attempts
             FROM agent_chat_turn_job
             WHERE status = 'leased' AND leased_until IS NOT NULL AND leased_until <= ?
             ORDER BY created_at ASC, id ASC",
        )
        .bind(&now)
        .fetch_all(self.db.pool())
        .await?;
        let decision_time = Utc::now();
        for row in expired {
            let id: String = row.try_get("id")?;
            let attempt_count: i64 = row.try_get("attempt_count")?;
            let max_attempts: i64 = row.try_get("max_attempts")?;
            let decision = failure_after_claim(
                attempt_count,
                max_attempts,
                decision_time,
                "Agent Chat lease expired",
            );
            let status = match decision.status {
                api_types::AgentChatTurnStatus::Failed => "failed",
                _ => "retry_wait",
            };
            sqlx::query(
                "UPDATE agent_chat_turn_job
                 SET status = ?, lease_owner = NULL, leased_until = NULL,
                     next_attempt_at = ?, error_code = 'lease_expired',
                     error_message = ?, version = version + 1, updated_at = ?
                 WHERE id = ? AND status = 'leased' AND leased_until IS NOT NULL
                   AND leased_until <= ?",
            )
            .bind(status)
            .bind(decision.next_attempt_at.map(|value| value.to_rfc3339()))
            .bind(decision.error)
            .bind(&now)
            .bind(id)
            .bind(&now)
            .execute(self.db.pool())
            .await?;
        }
        self.recover_parked().await?;
        Ok(())
    }

    async fn recover_parked(&self) -> Result<()> {
        let now = now_rfc3339();
        let parked_rows = sqlx::query(
            "SELECT id, pending_interaction_id, version
             FROM agent_chat_turn_job
             WHERE status = 'awaiting_input'
             ORDER BY created_at ASC, id ASC",
        )
        .fetch_all(self.db.pool())
        .await?;

        for row in parked_rows {
            let id: String = row.try_get("id")?;
            let pending_interaction_id: Option<String> = row.try_get("pending_interaction_id")?;
            let version: i64 = row.try_get("version")?;

            let Some(interaction_id) = pending_interaction_id else {
                let _ = sqlx::query(
                    "UPDATE agent_chat_turn_job
                     SET status = 'failed', error_code = 'interaction_orphaned',
                         error_message = 'turn parked without interaction reference',
                         version = version + 1, updated_at = ?
                     WHERE id = ? AND version = ? AND status = 'awaiting_input'",
                )
                .bind(&now)
                .bind(&id)
                .bind(version)
                .execute(self.db.pool())
                .await;
                continue;
            };

            let interaction =
                sqlx::query("SELECT status, expires_at FROM protected_interaction WHERE id = ?")
                    .bind(&interaction_id)
                    .fetch_optional(self.db.pool())
                    .await?;

            match interaction {
                None => {
                    let _ = sqlx::query(
                        "UPDATE agent_chat_turn_job
                         SET status = 'failed', error_code = 'interaction_orphaned',
                             error_message = 'pending interaction not found',
                             version = version + 1, updated_at = ?
                         WHERE id = ? AND version = ? AND status = 'awaiting_input'",
                    )
                    .bind(&now)
                    .bind(&id)
                    .bind(version)
                    .execute(self.db.pool())
                    .await;
                }
                Some(irow) => {
                    let istatus: String = irow.try_get("status")?;
                    let expires_at: Option<String> = irow.try_get("expires_at")?;
                    if istatus == "answered" {
                        let _ = sqlx::query(
                            "UPDATE agent_chat_turn_job
                             SET status = 'queued', pending_interaction_id = NULL,
                                 next_attempt_at = NULL, version = version + 1, updated_at = ?
                             WHERE id = ? AND version = ? AND status = 'awaiting_input'",
                        )
                        .bind(&now)
                        .bind(&id)
                        .bind(version)
                        .execute(self.db.pool())
                        .await;
                    } else if istatus == "cancelled" {
                        let _ = sqlx::query(
                            "UPDATE agent_chat_turn_job
                             SET status = 'cancelled', pending_interaction_id = NULL,
                                 error_code = 'interaction_cancelled',
                                 error_message = 'pending interaction was cancelled',
                                 version = version + 1, updated_at = ?
                             WHERE id = ? AND version = ? AND status = 'awaiting_input'",
                        )
                        .bind(&now)
                        .bind(&id)
                        .bind(version)
                        .execute(self.db.pool())
                        .await;
                    } else if istatus == "expired"
                        || (expires_at.as_deref().is_some()
                            && expires_at.as_deref().unwrap() <= now.as_str())
                    {
                        let _ = sqlx::query(
                            "UPDATE agent_chat_turn_job
                             SET status = 'failed', pending_interaction_id = NULL,
                                 error_code = 'interaction_expired',
                                 error_message = 'pending interaction expired',
                                 version = version + 1, updated_at = ?
                             WHERE id = ? AND version = ? AND status = 'awaiting_input'",
                        )
                        .bind(&now)
                        .bind(&id)
                        .bind(version)
                        .execute(self.db.pool())
                        .await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_claimed(&self, job: AgentChatTurnJob, cancellation: CancellationToken) {
        // RetryWait is a retry of this same logical admission, not a new turn.
        // Keep the claimed job (including its frozen responder/Profile/policy
        // provenance) as the runner input; claim/lease bookkeeping may change
        // status, attempt count, and version, but must never re-resolve the
        // current binding or Profile here.
        let stop = CancellationToken::new();
        let turn_cancellation = cancellation.child_token();
        let renewal =
            self.spawn_lease_renewal(job.id.clone(), stop.clone(), turn_cancellation.clone());
        let baseline_turn = job
            .operating_skill_revision_id
            .as_deref()
            .is_some_and(|revision| {
                revision == MAIN_BASELINE_OPERATING_SKILL_REVISION
                    || revision == LEGACY_MAIN_BASELINE_OPERATING_SKILL_REVISION
            });
        let mut control_transfer = None;
        let mut result = None;
        if baseline_turn {
            let run = self.runner.run_turn(&job, turn_cancellation.clone());
            tokio::pin!(run);
            tokio::select! {
                completed = &mut run => result = Some(completed),
                transfer = self.wait_for_genesis_control_transfer(&job.id) => {
                    control_transfer = Some(transfer);
                    // The typed command has already committed the durable
                    // continuation. Stop the baseline provider loop so it
                    // cannot add a redundant conversational response.
                    turn_cancellation.cancel();
                    let _ = tokio::time::timeout(Duration::from_secs(5), &mut run).await;
                }
            }
        } else {
            result = Some(self.runner.run_turn(&job, turn_cancellation).await);
        }
        stop.cancel();
        let _ = renewal.await;
        // Renewal is versioned. Re-read after the backend stops so a long
        // native turn commits against the current lease version rather than
        // the snapshot that was originally claimed.
        let commit_job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &job.id)
            .await
            .ok()
            .flatten()
            .unwrap_or(job.clone());
        if baseline_turn && control_transfer.is_none() {
            control_transfer = self
                .find_genesis_control_transfer(&commit_job.id)
                .await
                .ok()
                .flatten();
        }
        if let Some(transfer) = control_transfer {
            if let Err(error) = AgentChatTransactionRepo::complete_agent_chat_control_transfer(
                &*self.db,
                CompleteAgentChatControlTransfer {
                    turn_job_id: commit_job.id.clone(),
                    expected_version: commit_job.version,
                    lease_owner: self.lease_owner.clone(),
                    command_receipt_id: transfer.command_receipt_id,
                    continuation_turn_id: transfer.continuation_turn_id,
                    genesis_session_id: transfer.genesis_session_id,
                    updated_at: now_rfc3339(),
                },
            )
            .await
            {
                tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat Genesis control transfer commit failed");
            }
            return;
        }
        let Some(result) = result else {
            tracing::warn!(job_id = %commit_job.id, "Agent Chat runner stopped without a Genesis control transfer");
            return;
        };
        match result {
            Ok(turn) => {
                if let Some(pending_interaction_id) = turn.pending_interaction_id {
                    if let Err(error) = self
                        .chat_service
                        .append_awaiting_input(
                            &commit_job,
                            &self.lease_owner,
                            &pending_interaction_id,
                        )
                        .await
                    {
                        tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat awaiting input commit failed");
                        let _ = self
                            .chat_service
                            .append_failure(
                                &commit_job,
                                &self.lease_owner,
                                "awaiting_input_commit_failed",
                                "Agent Chat awaiting input state could not be committed",
                            )
                            .await;
                    }
                } else {
                    match self.turn_postcondition_satisfied(&commit_job).await {
                        Ok(true) => {
                            if let Err(error) = self.commit_success(&commit_job, turn).await {
                                tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat response commit failed");
                                let _ = self
                                    .chat_service
                                    .append_failure(
                                        &commit_job,
                                        &self.lease_owner,
                                        "response_commit_failed",
                                        "Agent Chat response could not be committed",
                                    )
                                    .await;
                            }
                        }
                        Ok(false) => {
                            tracing::warn!(
                                job_id = %commit_job.id,
                                "Agent Chat delivery follow-up postcondition was not committed"
                            );
                            let _ = self
                                .chat_service
                                .append_failure(
                                    &commit_job,
                                    &self.lease_owner,
                                    DELIVERY_FOLLOWUP_POSTCONDITION_FAILED,
                                    DELIVERY_FOLLOWUP_POSTCONDITION_MESSAGE,
                                )
                                .await;
                        }
                        Err(error) => {
                            tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat turn postcondition check failed");
                            let _ = self
                                .chat_service
                                .append_failure(
                                    &commit_job,
                                    &self.lease_owner,
                                    "turn_postcondition_check_failed",
                                    "Agent Chat turn postcondition could not be verified",
                                )
                                .await;
                        }
                    }
                }
            }
            Err(error) => {
                let code = classify_turn_error(&error);
                let message = bounded_error_message(&error.to_string());
                if let Err(commit_error) = self
                    .chat_service
                    .append_failure(&commit_job, &self.lease_owner, code, &message)
                    .await
                {
                    tracing::warn!(job_id = %commit_job.id, error = %commit_error, "Agent Chat failure could not be persisted");
                }
            }
        }
    }

    async fn wait_for_genesis_control_transfer(
        &self,
        source_turn_id: &str,
    ) -> GenesisControlTransfer {
        let mut interval = tokio::time::interval(Duration::from_millis(25));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match self.find_genesis_control_transfer(source_turn_id).await {
                Ok(Some(transfer)) => return transfer,
                Ok(None) => {}
                Err(error) => tracing::debug!(
                    turn_id = source_turn_id,
                    error = %error,
                    "Genesis control-transfer watch read failed"
                ),
            }
        }
    }

    async fn find_genesis_control_transfer(
        &self,
        source_turn_id: &str,
    ) -> Result<Option<GenesisControlTransfer>> {
        let row = sqlx::query(
            "SELECT id,
                    json_extract(outcome_json, '$.admitted_turn_id') AS continuation_turn_id,
                    json_extract(outcome_json, '$.session_id') AS genesis_session_id
             FROM command_receipt
             WHERE operation = ?
               AND json_extract(outcome_json, '$.source_turn_id') = ?
               AND json_extract(outcome_json, '$.control_transfer') = 1
             ORDER BY committed_at DESC, id DESC LIMIT 1",
        )
        .bind(MAIN_GENESIS_START_OPERATION)
        .bind(source_turn_id)
        .fetch_optional(self.db.pool())
        .await?;
        row.map(|row| {
            Ok(GenesisControlTransfer {
                command_receipt_id: row.try_get("id")?,
                continuation_turn_id: row.try_get("continuation_turn_id")?,
                genesis_session_id: row.try_get("genesis_session_id")?,
            })
        })
        .transpose()
    }

    async fn commit_success(
        &self,
        job: &AgentChatTurnJob,
        turn: CompletedAgentChatTurn,
    ) -> Result<CommittedAgentChatResponse> {
        self.chat_service
            .append_success(
                job,
                &self.lease_owner,
                AppendAgentChatSuccessInput {
                    content: turn.content,
                    model: turn.model,
                    session_id: Some(turn.session_id),
                    context_manifest_id: turn.context_manifest_id,
                    token_usage_json: turn.token_usage_json,
                    duration_ms: Some(turn.duration_ms),
                },
            )
            .await
    }

    async fn turn_postcondition_satisfied(&self, job: &AgentChatTurnJob) -> Result<bool> {
        let message =
            AgentChatMessageRepo::get_agent_chat_message(&*self.db, &job.triggering_message_id)
                .await?
                .filter(|message| message.chat_id == job.chat_id)
                .ok_or_else(|| {
                    ServiceError::not_found("agent_chat_message", job.triggering_message_id.clone())
                })?;
        let Some(postcondition) = delivery_followup_postcondition(&message)? else {
            return Ok(true);
        };
        let source_event_id = message.source_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("Delivery follow-up has no source wake event")
        })?;
        if job.causation_id.as_deref() != Some(source_event_id) {
            return Err(ServiceError::invalid_operation(
                "Delivery follow-up turn is not caused by its source wake event",
            ));
        }
        let canonical_wake_event = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM domain_event
             WHERE id = ? AND sequence = ? AND event_type = 'agent.wake.admitted'
               AND scope_type = 'project' AND scope_id = ?",
        )
        .bind(source_event_id)
        .bind(postcondition.after_event_sequence)
        .bind(&postcondition.required_scope_id)
        .fetch_optional(self.db.pool())
        .await?
        .is_some();
        if !canonical_wake_event {
            return Err(ServiceError::invalid_operation(
                "Delivery follow-up postcondition does not match its source wake event",
            ));
        }
        let canonical_project_chat = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM agent_chat
             WHERE id = ? AND kind = 'project' AND project_id = ?",
        )
        .bind(&job.chat_id)
        .bind(&postcondition.required_scope_id)
        .fetch_optional(self.db.pool())
        .await?
        .is_some();
        if !canonical_project_chat {
            return Err(ServiceError::invalid_operation(
                "Delivery follow-up postcondition does not match its canonical Project Chat",
            ));
        }
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM domain_event
             WHERE event_type = ? AND scope_type = ? AND scope_id = ?
               AND sequence > ?
             ORDER BY sequence ASC LIMIT 1",
        )
        .bind(&postcondition.required_event_type)
        .bind(&postcondition.required_scope_type)
        .bind(&postcondition.required_scope_id)
        .bind(postcondition.after_event_sequence)
        .fetch_optional(self.db.pool())
        .await?
        .is_some())
    }

    fn spawn_lease_renewal(
        &self,
        job_id: String,
        stop: CancellationToken,
        turn_cancellation: CancellationToken,
    ) -> JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let owner = self.lease_owner.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(LEASE_RENEW_INTERVAL);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = interval.tick() => {
                        let now = now_rfc3339();
                        let result = sqlx::query(
                            "UPDATE agent_chat_turn_job
                             SET leased_until = ?, version = version + 1, updated_at = ?
                             WHERE id = ? AND status = 'leased' AND lease_owner = ?",
                        )
                        .bind(lease_deadline())
                        .bind(&now)
                        .bind(&job_id)
                        .bind(&owner)
                        .execute(db.pool())
                        .await;
                        if result.map(|result| result.rows_affected() == 0).unwrap_or(true) {
                            turn_cancellation.cancel();
                            break;
                        }
                    }
                }
            }
        })
    }
}

#[derive(Debug, Deserialize)]
struct NativeProfileConfig {
    base_url: String,
    #[serde(default = "default_context_tokens")]
    context_tokens: u32,
    #[serde(default = "default_max_input_tokens")]
    max_input_tokens: u32,
    #[serde(default = "default_max_output_tokens")]
    max_output_tokens: u32,
}

fn default_context_tokens() -> u32 {
    128_000
}

fn default_max_input_tokens() -> u32 {
    96_000
}

fn default_max_output_tokens() -> u32 {
    16_000
}

fn runtime_history(history: &[AgentChatMessage]) -> Vec<Message> {
    history
        .iter()
        .map(|message| match message.author_type {
            AgentChatMessageAuthorType::User | AgentChatMessageAuthorType::Handoff => {
                Message::user(message.content.clone())
            }
            AgentChatMessageAuthorType::Agent => {
                Message::text(Role::Assistant, message.content.clone())
            }
            AgentChatMessageAuthorType::System => Message::system(message.content.clone()),
        })
        .collect()
}

fn delivery_followup_postcondition(
    message: &AgentChatMessage,
) -> Result<Option<DeliveryFollowupPostcondition>> {
    if message.author_type != AgentChatMessageAuthorType::System || message.source_type != "native"
    {
        return Ok(None);
    }
    let metadata = serde_json::from_str::<Value>(&message.source_metadata_json)
        .map_err(|_| ServiceError::invalid_operation("Agent Chat source metadata is invalid"))?;
    let Some(value) = metadata.get("turn_postcondition") else {
        return Ok(None);
    };
    if message.source_id.as_deref().is_none_or(str::is_empty) {
        return Err(ServiceError::invalid_operation(
            "Agent Chat turn postcondition is not server-authored",
        ));
    }
    let postcondition = serde_json::from_value::<DeliveryFollowupPostcondition>(value.clone())
        .map_err(|_| {
            ServiceError::invalid_operation("Agent Chat turn postcondition is malformed")
        })?;
    if postcondition.schema_version != DELIVERY_FOLLOWUP_POSTCONDITION_SCHEMA
        || postcondition.attention_id.trim().is_empty()
        || postcondition.required_event_type != DELIVERY_FOLLOWUP_READINESS_EVENT
        || postcondition.required_scope_type != "project"
        || postcondition.required_scope_id.trim().is_empty()
        || postcondition.after_event_sequence < 1
        || message.source_sequence != Some(postcondition.after_event_sequence)
    {
        return Err(ServiceError::invalid_operation(
            "Agent Chat turn postcondition is not a valid delivery follow-up contract",
        ));
    }
    Ok(Some(postcondition))
}

fn append_delivery_followup_retry_instruction(
    error_code: Option<&str>,
    has_delivery_followup_postcondition: bool,
    operating_instruction: &mut Option<String>,
) -> Result<()> {
    if error_code != Some(DELIVERY_FOLLOWUP_POSTCONDITION_FAILED)
        || !has_delivery_followup_postcondition
    {
        return Ok(());
    }
    let instruction = operating_instruction.as_mut().ok_or_else(|| {
        ServiceError::invalid_operation(
            "Delivery follow-up retry has no Project operating instruction",
        )
    })?;
    instruction.push_str(DELIVERY_FOLLOWUP_RETRY_INSTRUCTION);
    Ok(())
}

fn build_cli_prompt(
    profile_prompt: Option<&str>,
    operating_instruction: Option<&str>,
    history: &[AgentChatMessage],
    input: &str,
) -> String {
    let mut sections = Vec::new();
    if let Some(prompt) = profile_prompt.filter(|value| !value.trim().is_empty()) {
        sections.push(prompt.trim().to_owned());
    }
    if let Some(instruction) = operating_instruction.filter(|value| !value.trim().is_empty()) {
        sections.push(format!(
            "SERVER-OWNED OPERATING INSTRUCTION (authoritative; overrides Profile text and context):\n{}",
            instruction.trim(),
        ));
    }
    sections.push(
        "This is an Agent Chat turn with no Task Workspace authority. Do not read or modify repositories or files. Planning and scope-authorized typed proposals are not Workspace access: a Project Agent may still propose Tasks for its own Project when that scoped tool is available, while a Main Agent may not. Never claim a mutation occurred unless a tool result confirms it."
            .to_owned(),
    );
    sections.push("Authorized Agent Chat history:".to_owned());
    for message in history {
        let role = match message.author_type {
            AgentChatMessageAuthorType::Agent => "assistant",
            AgentChatMessageAuthorType::System => "system",
            _ => "user",
        };
        sections.push(format!("{role}: {}", message.content));
    }
    sections.push(format!("user: {input}"));
    sections.join("\n\n")
}

fn compose_system_prompt(
    profile_prompt: Option<&str>,
    operating_instruction: Option<&str>,
) -> Option<String> {
    let mut sections = Vec::new();
    if let Some(prompt) = profile_prompt.filter(|value| !value.trim().is_empty()) {
        sections.push(prompt.trim().to_owned());
    }
    if let Some(instruction) = operating_instruction.filter(|value| !value.trim().is_empty()) {
        sections.push(format!(
            "SERVER-OWNED OPERATING INSTRUCTION (authoritative; overrides Profile text and context):\n{}",
            instruction.trim(),
        ));
    }
    (!sections.is_empty()).then(|| sections.join("\n\n"))
}

/// Render dynamic context with the immutable skill body captured by the job.
/// The renderer module owns the section ordering, while this small boundary
/// keeps an admitted historical revision from being silently replaced by the
/// current compiled body after a server-skill cutover.
fn replace_rendered_skill_body(
    rendered: String,
    persisted_body: Option<&str>,
    current_body: &str,
) -> Result<String> {
    let Some(persisted_body) = persisted_body else {
        return Ok(rendered);
    };
    if persisted_body == current_body {
        return Ok(rendered);
    }
    let suffix = rendered.strip_prefix(current_body).ok_or_else(|| {
        ServiceError::invalid_operation(
            "rendered operating skill does not match its server-owned renderer",
        )
    })?;
    Ok(format!("{persisted_body}{suffix}"))
}

/// Select the Main renderer branch from admission provenance alone.  The
/// current Product Genesis lifecycle is intentionally absent: a lifecycle
/// transition after admission must not turn a baseline job into discovery (or
/// the reverse) while it waits in the queue.
fn main_skill_key_for_frozen_revision(revision_id: &str) -> &'static str {
    if revision_id == MAIN_BASELINE_OPERATING_SKILL_REVISION
        || revision_id == LEGACY_MAIN_BASELINE_OPERATING_SKILL_REVISION
    {
        MAIN_BASELINE_OPERATING_SKILL_KEY
    } else {
        MAIN_OPERATING_SKILL_KEY
    }
}

fn agent_chat_manifest_id(
    identity_id: &str,
    session_id: &str,
    runtime_manifest: &RuntimeContextManifestLink,
) -> uuid::Uuid {
    let mut digest = Sha256::new();
    digest.update(b"forge-agent-chat-context-manifest-v1\0");
    digest.update(identity_id.as_bytes());
    digest.update([0]);
    digest.update(session_id.as_bytes());
    digest.update([0]);
    digest.update(runtime_manifest.turn_id.as_bytes());
    let bytes = digest.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&bytes[..16]);
    id[6] = (id[6] & 0x0f) | 0x50;
    id[8] = (id[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(id)
}

fn agent_chat_server_manifest_id(identity_id: &str, session_id: &str, job_id: &str) -> uuid::Uuid {
    let mut digest = Sha256::new();
    digest.update(b"forge-agent-chat-server-context-manifest-v1\0");
    digest.update(identity_id.as_bytes());
    digest.update([0]);
    digest.update(session_id.as_bytes());
    digest.update([0]);
    digest.update(job_id.as_bytes());
    let bytes = digest.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&bytes[..16]);
    id[6] = (id[6] & 0x0f) | 0x50;
    id[8] = (id[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(id)
}

fn canonical_operating_context_sources(
    operating_context_sources: &[ContextSourceInput],
) -> Vec<Value> {
    // ContextSourceInput is an internal persistence DTO. Keep the digest
    // contract explicit and bounded instead of making its Rust serialization
    // shape part of the request wire format (or accidentally admitting a
    // future field such as a context body).
    operating_context_sources
        .iter()
        .map(|source| {
            serde_json::json!({
                "ordinal": source.ordinal,
                "source_id": &source.source_id,
                "source_type": &source.source_type,
                "source_revision": &source.source_revision,
                "selection_reason": &source.selection_reason,
                "disposition": &source.disposition,
                "retention_priority": source.retention_priority,
                "fragment_fingerprint": &source.fragment_fingerprint,
                "sensitivity": &source.sensitivity,
            })
        })
        .collect()
}

fn agent_chat_request_fingerprint(
    job: &AgentChatTurnJob,
    profile: &AgentProfile,
    session: &AgentSession,
    model: Option<&str>,
    runtime_manifest: &RuntimeContextManifestLink,
    operating_context_sources: &[ContextSourceInput],
) -> Result<String> {
    let operating_context_sources = canonical_operating_context_sources(operating_context_sources);
    api_types::canonical_digest_with_schema(
        "forge.agent-chat-runtime-request/v1",
        &serde_json::json!({
            "job": {
                "chat_id": &job.chat_id,
                "triggering_message_id": &job.triggering_message_id,
                "correlation_id": &job.correlation_id,
                "causation_depth": job.causation_depth,
            },
            "session": {
                "id": &session.id,
                "context_scope_id": &session.context_scope_id,
            },
            "profile": {
                "id": &profile.id,
                "version": profile.version,
                "backend_kind": &profile.backend_kind,
                "model": model,
            },
            "runtime_manifest": {
                "turn_id": &runtime_manifest.turn_id,
                "context_fingerprint": &runtime_manifest.context_fingerprint,
                "cache_plan_fingerprint": &runtime_manifest.cache_plan_fingerprint,
                "runtime_manifest_fingerprint": &runtime_manifest.runtime_manifest_fingerprint,
            },
            "operating_context_sources": operating_context_sources,
        }),
    )
    .map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Agent Chat runtime request cannot be canonically serialized: {error}"
        ))
    })
}

fn agent_chat_server_request_fingerprint(
    job: &AgentChatTurnJob,
    profile: &AgentProfile,
    session: &AgentSession,
    model: Option<&str>,
    operating_context_sources: &[ContextSourceInput],
) -> Result<String> {
    let operating_context_sources = canonical_operating_context_sources(operating_context_sources);
    api_types::canonical_digest_with_schema(
        "forge.agent-chat-server-context-request/v1",
        &serde_json::json!({
            "job": {
                "id": &job.id,
                "chat_id": &job.chat_id,
                "triggering_message_id": &job.triggering_message_id,
                "correlation_id": &job.correlation_id,
                "causation_depth": job.causation_depth,
            },
            "session": {
                "id": &session.id,
                "context_scope_id": &session.context_scope_id,
            },
            "profile": {
                "id": &profile.id,
                "version": profile.version,
                "backend_kind": &profile.backend_kind,
                "model": model,
            },
            "operating_context_sources": operating_context_sources,
        }),
    )
    .map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Agent Chat server context request cannot be canonically serialized: {error}"
        ))
    })
}

fn runtime_manifest_sources(
    runtime_manifest: &RuntimeContextManifestLink,
) -> Vec<ContextSourceInput> {
    let source_revision = runtime_manifest.context_fingerprint.clone();
    let covered = runtime_manifest
        .summaries
        .iter()
        .flat_map(|summary| summary.covered.iter().cloned())
        .collect::<BTreeSet<_>>();
    let summary_ids = runtime_manifest
        .summaries
        .iter()
        .map(|summary| summary.summary.clone())
        .collect::<BTreeSet<_>>();
    let segment_ids = runtime_manifest
        .segments
        .iter()
        .map(|segment| segment.id.clone())
        .collect::<BTreeSet<_>>();
    let mut sources = Vec::new();
    let mut source_ids = BTreeSet::new();
    let mut ordinal = 0_i64;
    let mut push = |source_id: String,
                    source_type: String,
                    source_revision: String,
                    selection_reason: String,
                    disposition: String,
                    retention_priority: i64,
                    fragment_fingerprint: String,
                    sensitivity: String| {
        if source_ids.insert(source_id.clone()) {
            sources.push(ContextSourceInput {
                ordinal,
                source_id,
                source_type,
                source_revision,
                selection_reason,
                disposition,
                retention_priority,
                fragment_fingerprint,
                sensitivity,
            });
            ordinal = ordinal.saturating_add(1);
        }
    };

    if let Some(timeline_id) = runtime_manifest.lcm_timeline_id.as_deref() {
        push(
            timeline_id.to_owned(),
            "runtime_lcm_timeline".to_owned(),
            runtime_manifest
                .lcm_binding_revision
                .clone()
                .unwrap_or_else(|| "unknown".to_owned()),
            "agent_runtime_lcm_binding".to_owned(),
            "included".to_owned(),
            100,
            fingerprint_id(timeline_id),
            "internal".to_owned(),
        );
    }
    for segment in &runtime_manifest.segments {
        push(
            segment.id.clone(),
            "runtime_segment".to_owned(),
            source_revision.clone(),
            "agent_runtime_final_segment".to_owned(),
            if covered.contains(&segment.id) && !summary_ids.contains(&segment.id) {
                "summarized".to_owned()
            } else {
                "included".to_owned()
            },
            if summary_ids.contains(&segment.id) {
                100
            } else {
                10
            },
            segment.content_hash.clone(),
            segment.sensitivity.clone(),
        );
    }
    for summary in &runtime_manifest.summaries {
        push(
            summary.summary.clone(),
            "runtime_lcm_summary".to_owned(),
            source_revision.clone(),
            "agent_runtime_summary_coverage".to_owned(),
            "included".to_owned(),
            100,
            fingerprint_id(&summary.summary),
            "sensitive".to_owned(),
        );
        for covered_id in &summary.covered {
            push(
                covered_id.clone(),
                "runtime_lcm_covered".to_owned(),
                source_revision.clone(),
                "agent_runtime_summary_coverage".to_owned(),
                "summarized".to_owned(),
                10,
                fingerprint_id(covered_id),
                "sensitive".to_owned(),
            );
        }
    }
    for summary in &runtime_manifest.lossless_summaries {
        let source_id = summary.node_id.clone();
        push(
            source_id.clone(),
            "runtime_lossless_summary".to_owned(),
            summary.node_revision.to_string(),
            "agent_runtime_lossless_summary".to_owned(),
            "included".to_owned(),
            100,
            summary
                .operation_fingerprint
                .clone()
                .unwrap_or_else(|| summary.source_fingerprint.clone()),
            summary.classification.sensitivity.clone(),
        );
    }
    // Keep the local variable meaningful in the no-summary case and make the
    // dedupe rule explicit for reviewers: source IDs are never repeated.
    let _ = segment_ids;
    sources
}

fn fingerprint_id(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn canonical_context_digest<T: Serialize>(value: &T) -> Result<String> {
    api_types::canonical_digest_with_schema(PROJECT_CONTEXT_DIGEST_SCHEMA_VERSION, value).map_err(
        |error| {
            ServiceError::invalid_operation(format!(
                "Project context reference cannot be canonically serialized: {error}"
            ))
        },
    )
}

fn validate_project_handoff_packet(
    source_revisions_json: &str,
    expected: &ProjectHandoffExpectation<'_>,
    approval_lifecycle: &str,
    consumed_project_id: Option<&str>,
    project_id: &str,
) -> Result<ProjectCharterHandoffPacket> {
    if approval_lifecycle != "consumed" || consumed_project_id != Some(project_id) {
        return Err(ServiceError::invalid_operation(
            "Project Agent handoff requires the consumed Charter approval for this Project",
        ));
    }
    let packet = parse_project_handoff_packet(source_revisions_json)?;
    let packet_value: Value = serde_json::from_str(source_revisions_json).map_err(|_| {
        ServiceError::invalid_operation("Project Agent handoff has an invalid source manifest")
    })?;
    let source_manifest: Value = serde_json::from_str(&packet.request.source_revisions_json)
        .map_err(|_| {
            ServiceError::invalid_operation(
                "Project Agent handoff request source manifest is invalid",
            )
        })?;
    if !source_manifest.is_object() {
        return Err(ServiceError::invalid_operation(
            "Project Agent handoff request source manifest must be an object",
        ));
    }
    if !handoff_source_manifest_matches_packet(&source_manifest, &packet) {
        return Err(ServiceError::invalid_operation(
            "Project Agent handoff request source manifest does not match its canonical packet",
        ));
    }
    let recomputed_source_revisions_digest =
        handoff_request_fingerprint(&packet_value, &packet.request.authorization)?;
    let redaction_categories = {
        let mut categories = packet.redaction_manifest.excluded_categories.clone();
        categories.sort();
        categories
    };
    let mut required_categories = REQUIRED_HANDOFF_REDACTION_CATEGORIES
        .iter()
        .map(|category| (*category).to_owned())
        .collect::<Vec<_>>();
    required_categories.sort();
    let values_are_bounded = packet.bounded_summary.chars().count() <= MAX_HANDOFF_BOUNDED_CHARS
        && packet.settled_decision_ids.len() <= 64
        && packet.unresolved_items.len() <= 64
        && packet.research_references.len() <= 64
        && packet
            .unresolved_items
            .iter()
            .chain(packet.research_references.iter())
            .all(handoff_value_is_bounded)
        && handoff_text_is_safe(&packet.bounded_summary)
        && packet
            .settled_decision_ids
            .iter()
            .all(|value| non_empty(value) && handoff_text_is_safe(value));
    let redaction_shape_is_valid = redaction_categories == required_categories
        && packet
            .redaction_manifest
            .excluded_knowledge_item_ids
            .iter()
            .all(|value| non_empty(value))
        && packet
            .redaction_manifest
            .excluded_knowledge_item_ids
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            == packet.redaction_manifest.excluded_knowledge_item_ids.len();
    let matches = values_are_bounded
        && redaction_shape_is_valid
        && packet.schema_version == PROJECT_HANDOFF_SCHEMA_VERSION
        && packet.handoff_id == expected.handoff_id
        && packet.deduplication_key == expected.deduplication_key
        && packet.correlation_id == expected.correlation_id
        && packet.causation_id == expected.causation_id
        && packet.approval_id == expected.approval_id
        && non_empty(&packet.request.policy_revision)
        && packet.request.policy_revision == expected.policy_revision
        && non_empty(&packet.request.policy_digest)
        && packet.request.policy_digest == expected.policy_digest
        && non_empty(&packet.request.source_revisions_digest)
        && packet.request.source_revisions_digest == recomputed_source_revisions_digest
        && non_empty(&packet.request.source_revisions_json)
        && non_empty(&packet.request.authorization.principal_type)
        && packet.request.authorization.principal_type == expected.approval_principal_kind
        && non_empty(&packet.request.authorization.principal_id)
        && packet.request.authorization.principal_id == expected.approval_principal_id
        && packet.request.authorization.principal_type
            == expected.create_authorization_principal_type
        && packet.request.authorization.principal_id == expected.create_authorization_principal_id
        && packet.request.authorization.authorization_basis == expected.create_authorization_basis
        && packet.request.authorization.action == expected.create_authorization_action
        && packet.request.authorization.event_id == expected.create_authorization_event_id
        && packet.request.authorization.occurred_at == expected.create_authorization_occurred_at
        && packet.source.chat_id == expected.source_chat_id
        && packet.source.identity_id == expected.source_identity_id
        && packet.source.profile_revision_id == expected.source_profile_revision_id
        && packet.source.instruction_revision_id == expected.source_instruction_revision_id
        && packet.source.instruction_revision == expected.source_instruction_revision
        && packet.source.message_ids == expected.source_message_ids
        && packet
            .source
            .message_ids
            .last()
            .is_some_and(|last| last == &packet.source.message_id)
        && packet.source.turn_id.as_deref() == expected.source_turn_id
        && packet.project.id == expected.project_id
        && packet.project.name == expected.project_name
        && packet.project.lifecycle == "active"
        && packet.project.mode == expected.project_mode
        && packet.project.approved_slug.as_deref() == expected.approved_slug
        && packet.target.chat_id == expected.target_chat_id
        && packet.target.binding_id == expected.target_binding_id
        && packet.target.message_id == expected.target_message_id
        && packet.target.turn_id == expected.target_turn_id
        && packet.target.identity_id == expected.identity_id
        && packet.target.profile_revision_id == expected.profile_revision_id
        && packet.charter.id == expected.charter_id
        && packet.charter.revision_id == expected.charter_revision_id
        && packet.charter.revision_number == expected.charter_revision_number
        && packet.charter.schema_version == expected.charter_schema_version
        && packet.charter.content_digest == expected.charter_content_digest
        && packet.charter.render_version == expected.charter_render_version
        && packet.charter.render_digest == expected.charter_render_digest
        && packet.approval.id == expected.approval_id
        && packet.approval.event_id == expected.approval_event_id
        && packet.approval.authorization_basis == expected.approval_authorization_basis
        && packet.approval.authorization_action == expected.approval_authorization_action
        && packet.approval.authorization_event_id == expected.approval_authorization_event_id
        && packet.approval.authorization_occurred_at == expected.approval_authorization_occurred_at
        && packet.approval.approved_by.kind == expected.approval_principal_kind
        && packet.approval.approved_by.id == expected.approval_principal_id
        && packet.approval.approved_at == expected.approval_created_at
        && packet.project_agent.identity_id == expected.identity_id
        && packet.project_agent.profile_revision_id == expected.profile_revision_id
        // The immutable packet records the skill revision current at handoff
        // time. The skill body is server-owned; the binding must carry the
        // exact current server contract (validated separately), so the packet
        // only needs to name a revision of the same server-owned skill key.
        && packet
            .project_agent
            .operating_skill_revision
            .starts_with(&format!("{PROJECT_OPERATING_SKILL_KEY}@"))
        && packet.project_agent.policy_revision == expected.policy_revision
        && packet.project_agent.policy_digest == expected.policy_digest
        && packet.content_classification == "approved_project_charter"
        && packet.created_at == expected.created_at
        && packet.delivery.delivered_at == expected.delivered_at
        && non_empty(&packet.handoff_id)
        && non_empty(&packet.deduplication_key)
        && non_empty(&packet.correlation_id)
        && non_empty(&packet.causation_id)
        && non_empty(&packet.source.chat_id)
        && non_empty(&packet.source.identity_id)
        && non_empty(&packet.source.profile_revision_id)
        && non_empty(&packet.source.instruction_revision_id)
        && non_empty(&packet.source.message_id)
        && packet
            .source
            .message_ids
            .iter()
            .all(|value| non_empty(value))
        && non_empty(&packet.project.id)
        && non_empty(&packet.project.name)
        && non_empty(&packet.target.chat_id)
        && non_empty(&packet.target.binding_id)
        && non_empty(&packet.target.identity_id)
        && non_empty(&packet.target.profile_revision_id)
        && non_empty(&packet.target.message_id)
        && non_empty(&packet.target.turn_id)
        && non_empty(&packet.charter.id)
        && non_empty(&packet.charter.revision_id)
        && non_empty(&packet.charter.schema_version)
        && non_empty(&packet.charter.content_digest)
        && non_empty(&packet.charter.render_version)
        && non_empty(&packet.charter.render_digest)
        && non_empty(&packet.approval.id)
        && non_empty(&packet.approval.event_id)
        && non_empty(&packet.approval.authorization_basis)
        && non_empty(&packet.approval.authorization_action)
        && non_empty(&packet.approval.authorization_event_id)
        && non_empty(&packet.approval.authorization_occurred_at)
        && non_empty(&packet.approval.approved_by.kind)
        && non_empty(&packet.approval.approved_by.id)
        && non_empty(&packet.approval.approved_at)
        && non_empty(&packet.project_agent.identity_id)
        && non_empty(&packet.project_agent.profile_revision_id)
        && non_empty(&packet.project_agent.operating_skill_revision)
        && non_empty(&packet.project_agent.policy_revision)
        && non_empty(&packet.project_agent.policy_digest)
        && non_empty(&packet.content_classification)
        && non_empty(&packet.created_at)
        && non_empty(&packet.delivery.delivered_at);
    if !matches {
        return Err(ServiceError::invalid_operation(
            "Project Agent handoff Charter, approval, selected agent, or policy references are stale or mismatched",
        ));
    }
    Ok(packet)
}

fn parse_project_handoff_packet(
    source_revisions_json: &str,
) -> Result<ProjectCharterHandoffPacket> {
    serde_json::from_str(source_revisions_json).map_err(|_| {
        ServiceError::invalid_operation("Project Agent handoff has an invalid typed Charter packet")
    })
}

fn handoff_source_manifest_matches_packet(
    source_manifest: &Value,
    packet: &ProjectCharterHandoffPacket,
) -> bool {
    let Ok(mut expected) = serde_json::to_value(packet) else {
        return false;
    };
    let Some(expected) = expected.as_object_mut() else {
        return false;
    };
    expected.remove("request");
    expected.remove("approval_id");
    if let Some(target) = expected.get_mut("target").and_then(Value::as_object_mut) {
        target.insert("chat_id".to_owned(), Value::Null);
    }
    if let Some(source) = expected.get_mut("source").and_then(Value::as_object_mut) {
        source.remove("message_id");
    }
    if let Some(delivery) = expected.get_mut("delivery").and_then(Value::as_object_mut) {
        delivery.insert("delivered_at".to_owned(), Value::Null);
    }
    canonicalize_project_handoff_json(&Value::Object(expected.clone()))
        == canonicalize_project_handoff_json(source_manifest)
}

/// Reproduce the DB transaction's immutable handoff-request fingerprint after
/// removing values allocated by that transaction.  The fingerprint therefore
/// proves the complete bounded packet shape and the exact authorization
/// envelope without treating a caller-controlled digest as authority.
fn handoff_request_fingerprint(
    value: &Value,
    authorization: &ProjectCharterHandoffAuthorization,
) -> Result<String> {
    let source_revisions_json = value
        .pointer("/request/source_revisions_json")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent handoff request source manifest is required",
            )
        })?;
    let authorization = serde_json::to_value(authorization).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Project Agent handoff authorization is invalid: {error}"
        ))
    })?;
    project_handoff_request_fingerprint(value, source_revisions_json, &authorization)
        .map_err(ServiceError::invalid_operation)
}

fn non_empty(value: &str) -> bool {
    !value.trim().is_empty()
}

fn handoff_text_is_safe(value: &str) -> bool {
    if value.chars().count() > MAX_HANDOFF_BOUNDED_CHARS {
        return false;
    }
    if value.trim().is_empty() {
        return true;
    }
    guard_agent_chat_content(value).is_ok()
}

fn handoff_value_is_bounded(value: &Value) -> bool {
    let Ok(serialized) = serde_json::to_string(value) else {
        return false;
    };
    serialized.chars().count() <= MAX_HANDOFF_BOUNDED_CHARS && handoff_text_is_safe(&serialized)
}

fn project_agent_policy_digest(tool_policy_json: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"forge.project-agent-policy/v1\0");
    digest.update(tool_policy_json.as_bytes());
    hex::encode(digest.finalize())
}

fn effective_state_context(
    projection: &ProjectEffectiveStateProjection,
) -> Result<EffectiveProjectStateContext> {
    let mut reconciliation_required = projection
        .reconciliation_required
        .iter()
        .map(|record| {
            format!(
                "{}:{}:{}@{}:digest:{}:state:{}:v{}:updated:{}",
                record.id,
                record.record_type,
                record.record_id,
                record.record_revision,
                record.record_digest,
                record.state,
                record.version,
                record.updated_at
            )
        })
        .collect::<Vec<_>>();
    let unreleased = &projection.unreleased_changes;
    if !unreleased.document_ids.is_empty()
        || !unreleased.decision_candidate_ids.is_empty()
        || !unreleased.baseline_revision_ids.is_empty()
        || !unreleased.active_milestone_ids.is_empty()
        || !unreleased.reconciliation_ids.is_empty()
    {
        reconciliation_required.push(format!(
            "unreleased changes: documents={:?}; decision_candidates={:?}; baseline_revisions={:?}; active_milestones={:?}; reconciliations={:?}",
            unreleased.document_ids,
            unreleased.decision_candidate_ids,
            unreleased.baseline_revision_ids,
            unreleased.active_milestone_ids,
            unreleased.reconciliation_ids,
        ));
    }
    if !projection.commitments.is_empty() || !projection.inbox.is_empty() {
        reconciliation_required.push(format!(
            "coordination reconciliation: commitments={:?}; inbox={:?}",
            projection
                .commitments
                .iter()
                .map(|commitment| {
                    format!(
                        "{}:{}@v{}:evidence={}",
                        commitment.id,
                        commitment.status,
                        commitment.version,
                        commitment.evidence_count
                    )
                })
                .collect::<Vec<_>>(),
            projection
                .inbox
                .iter()
                .map(|item| format!(
                    "{}:{}:{}@v{}",
                    item.id, item.kind, item.status, item.version
                ))
                .collect::<Vec<_>>(),
        ));
    }
    Ok(EffectiveProjectStateContext {
        governing_charter: projection.governing_charter.as_ref().map(|charter| {
            format!(
                "{}@{}#r{}:content:{}:render:{}:v{}",
                charter.id,
                charter.revision_id,
                charter.revision,
                charter.content_digest,
                charter.render_digest,
                charter.version
            )
        }),
        active_execution_baseline: projection
            .active_execution_baseline
            .as_ref()
            .map(|baseline| {
                format!(
                    "{}@{}:content:{}:render:{}:policy:{}@{}",
                    baseline.id,
                    baseline.revision_id,
                    baseline.content_digest,
                    baseline.render_digest,
                    baseline.release_policy_revision,
                    baseline.release_policy_digest
                )
            }),
        applicable_document_revisions: projection
            .approved_documents
            .iter()
            .map(|document| {
                format!(
                    "{}:{}@{}:content:{}:render:{}",
                    document.kind,
                    document.id,
                    document.revision_id,
                    document.content_digest,
                    document.render_digest
                )
            })
            .collect(),
        active_decisions: projection
            .active_decisions
            .iter()
            .map(|decision| {
                let digest = canonical_context_digest(decision)?;
                Ok::<_, ServiceError>(format!(
                    "{} [{}] @{}: {}:digest:{}{}",
                    decision.id,
                    decision.decision_class,
                    decision.created_at,
                    decision.selected_outcome,
                    digest,
                    decision
                        .baseline_revision_id
                        .as_deref()
                        .map(|revision| format!(":baseline:{revision}"))
                        .unwrap_or_default()
                ))
            })
            .collect::<Result<Vec<_>>>()?,
        reconciliation_required,
        canonical_conflicts: projection
            .canonical_conflicts
            .iter()
            .map(|conflict| {
                format!(
                    "{} [{}] {}:{}@{}:{}@{}:digests:{}:{}: {}",
                    conflict.id,
                    conflict.domain,
                    conflict.governing_record_type,
                    conflict.governing_record_id,
                    conflict.governing_record_revision,
                    conflict.conflicting_record_type,
                    conflict.conflicting_record_revision,
                    conflict.governing_record_digest,
                    conflict.conflicting_record_digest,
                    conflict.description
                )
            })
            .collect(),
        task_summary: projection
            .task_summary
            .by_status
            .iter()
            .map(|count| format!("{}={}", count.key, count.count))
            .collect::<Vec<_>>()
            .join(", ")
            + &format!(" (total={})", projection.task_summary.total),
        validation_summary: projection
            .validation_summary
            .by_outcome
            .iter()
            .map(|count| format!("{}={}", count.key, count.count))
            .collect::<Vec<_>>()
            .join(", ")
            + &format!(" (total={})", projection.validation_summary.total),
        active_milestones: projection
            .active_milestones
            .iter()
            .map(|milestone| {
                let definition_revision =
                    milestone.definition_revision_id.as_deref().ok_or_else(|| {
                        ServiceError::conflict(
                            "active Project milestone has no definition revision",
                        )
                    })?;
                let definition_digest =
                    milestone.definition_digest.as_deref().ok_or_else(|| {
                        ServiceError::conflict("active Project milestone has no definition digest")
                    })?;
                Ok::<_, ServiceError>(format!(
                    "{} ({}) @{}:content:{}:v{}",
                    milestone.milestone_key,
                    milestone.lifecycle,
                    definition_revision,
                    definition_digest,
                    milestone.version
                ))
            })
            .collect::<Result<Vec<_>>>()?,
        primary_milestone_id: projection.primary_milestone_id.clone(),
        readiness: projection
            .readiness
            .latest
            .as_ref()
            .map(|readiness| {
                format!(
                    "{} ({}@{}):event:{}:baseline:{}@{}:content:{}:policy:{}@{}",
                    readiness.outcome,
                    readiness.id,
                    readiness.readiness_digest,
                    readiness.event_watermark,
                    readiness.baseline_id,
                    readiness.baseline_revision_id,
                    readiness.baseline_digest,
                    readiness.release_policy_revision,
                    readiness.release_policy_digest
                )
            })
            .unwrap_or_else(|| "unresolved".to_owned()),
        releases: projection
            .releases
            .iter()
            .map(|release| {
                format!(
                    "{} ({}@{}):snapshot:{}:baseline:{}@{}:content:{}:created:{}",
                    release.release_identifier,
                    release.id,
                    release.readiness_digest,
                    release.snapshot_digest,
                    release.baseline_id,
                    release.baseline_revision_id,
                    release.baseline_digest,
                    release.created_at
                )
            })
            .collect(),
        event_watermark: Some(projection.source_event_watermark.clone()),
    })
}

fn hash_parts<'a, I>(prefix: &[u8], values: I) -> String
where
    I: IntoIterator<Item = &'a String>,
{
    let mut digest = Sha256::new();
    digest.update(prefix);
    for value in values {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}

fn project_operating_context_sources(
    skill_revision_id: &str,
    skill_content_digest: &str,
    context_references: &[OperatingContextReference],
) -> Vec<ContextSourceInput> {
    let mut sources = Vec::with_capacity(context_references.len() + 1);
    sources.push(ContextSourceInput {
        ordinal: 0,
        source_id: format!("operating_skill:{PROJECT_OPERATING_SKILL_KEY}"),
        source_type: "server_operating_skill".to_owned(),
        source_revision: skill_revision_id.to_owned(),
        selection_reason: "server_owned_project_operating_skill".to_owned(),
        disposition: "included".to_owned(),
        retention_priority: 100,
        // The renderer body is immutable and its canonical digest is the
        // source fingerprint.  A key/revision hash would prove only that the
        // pointer was formatted, not that the selected body was authentic.
        fragment_fingerprint: skill_content_digest.to_owned(),
        sensitivity: "internal".to_owned(),
    });
    for (ordinal, reference) in context_references.iter().enumerate() {
        sources.push(ContextSourceInput {
            ordinal: ordinal as i64 + 1,
            source_id: format!(
                "project_context:{}:{}",
                reference.source_type, reference.source_id
            ),
            source_type: reference.source_type.clone(),
            source_revision: reference.source_revision.clone(),
            selection_reason: reference.selection_reason.clone(),
            disposition: reference.disposition.clone(),
            retention_priority: 100,
            fragment_fingerprint: reference.digest.clone(),
            sensitivity: reference.sensitivity.clone(),
        });
    }
    sources
}

fn main_operating_context_sources(
    skill_key: &str,
    skill_revision_id: &str,
    skill_content_digest: &str,
    selection_reason: &str,
    context_prefix: &str,
    context_references: &[OperatingContextReference],
) -> Vec<ContextSourceInput> {
    let mut sources = Vec::with_capacity(context_references.len() + 1);
    sources.push(ContextSourceInput {
        ordinal: 0,
        source_id: format!("operating_skill:{skill_key}"),
        source_type: "server_operating_skill".to_owned(),
        source_revision: skill_revision_id.to_owned(),
        selection_reason: selection_reason.to_owned(),
        disposition: "included".to_owned(),
        retention_priority: 100,
        fragment_fingerprint: skill_content_digest.to_owned(),
        sensitivity: "internal".to_owned(),
    });
    for (ordinal, reference) in context_references.iter().enumerate() {
        sources.push(ContextSourceInput {
            ordinal: ordinal as i64 + 1,
            source_id: format!(
                "{context_prefix}:{}:{}",
                reference.source_type, reference.source_id
            ),
            source_type: reference.source_type.clone(),
            source_revision: reference.source_revision.clone(),
            selection_reason: reference.selection_reason.clone(),
            disposition: reference.disposition.clone(),
            retention_priority: 100,
            fragment_fingerprint: reference.digest.clone(),
            sensitivity: reference.sensitivity.clone(),
        });
    }
    sources
}

fn cli_result_content(result: ExecutionResult) -> Result<String> {
    let content = match result.status {
        ExecutionOutcome::Completed => result
            .assistant_output
            .or(result.summary)
            .filter(|summary| !summary.trim().is_empty())
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat CLI returned no content")),
        ExecutionOutcome::Cancelled => Err(ServiceError::invalid_operation(
            "Agent Chat CLI turn was cancelled",
        )),
        ExecutionOutcome::Failed => Err(ServiceError::invalid_operation(
            "Agent Chat CLI turn failed",
        )),
    }?;
    Ok(if content.chars().count() <= MAX_CLI_ASSISTANT_CHARS {
        content
    } else {
        content.chars().take(MAX_CLI_ASSISTANT_CHARS).collect()
    })
}

fn cli_profile_execution_config(profile: &AgentProfile) -> Result<Value> {
    let mut config: Value = serde_json::from_str(&profile.config_json)
        .map_err(|_| ServiceError::invalid_operation("Agent profile config is invalid"))?;
    merge_overrides(
        &mut config,
        &ExecutionOverrides {
            model_id: profile.model.clone(),
            reasoning_effort: profile.reasoning_effort.clone(),
            permission_policy: profile.permission_policy.clone(),
        },
    )?;

    if profile.executor_type == ExecutorKind::Smith.to_string() {
        if let Some(provider) = &profile.provider {
            config
                .as_object_mut()
                .expect("merge_overrides accepted an object")
                .insert("provider".to_owned(), Value::String(provider.clone()));
        }
    }

    Ok(config)
}

fn cli_executor_snapshot(executor_type: &str, config: Value) -> Value {
    serde_json::json!({
        "executor_type": executor_type,
        "config": config,
    })
}

fn lease_deadline() -> String {
    (Utc::now() + ChronoDuration::seconds(TURN_LEASE_SECONDS)).to_rfc3339()
}

fn chat_sandbox_path(job_id: &str) -> PathBuf {
    std::env::temp_dir()
        .join("forge-agent-chat-sandboxes")
        .join(job_id)
}

fn chat_log_path(job_id: &str) -> PathBuf {
    std::env::temp_dir()
        .join("forge-agent-chat-logs")
        .join(format!("{job_id}.jsonl"))
}

fn bounded_error_message(value: &str) -> String {
    value.chars().take(MAX_ERROR_CHARS).collect()
}

fn classify_turn_error(error: &ServiceError) -> &'static str {
    let text = error.to_string().to_ascii_lowercase();
    if text.contains("usage limit") || text.contains("limit exhausted") {
        "usage_limit"
    } else if text.contains("credential") {
        "credential_unavailable"
    } else if text.contains("cancel") {
        "cancelled"
    } else if text.contains("scope")
        || text.contains("permission")
        || text.contains("binding")
        || text.contains("charter")
        || text.contains("operating-skill")
        || text.contains("handoff")
    {
        "authority_denied"
    } else if text.contains("profile") || text.contains("config") {
        "configuration_invalid"
    } else {
        "backend_failed"
    }
}

#[derive(Debug)]
struct NoopTurnEventSink;

#[async_trait]
impl TurnEventSink for NoopTurnEventSink {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_sandbox_is_job_scoped() {
        assert!(chat_sandbox_path("job-a") != chat_sandbox_path("job-b"));
        assert!(chat_log_path("job-a") != chat_log_path("job-b"));
    }

    #[test]
    fn frozen_main_branch_ignores_later_genesis_lifecycle_changes() {
        // The branch is selected from the immutable admission marker.  A
        // lifecycle flip (baseline -> discovery or discovery -> handed_off)
        // therefore cannot silently change a queued turn's server contract.
        assert_eq!(
            main_skill_key_for_frozen_revision(MAIN_BASELINE_OPERATING_SKILL_REVISION),
            MAIN_BASELINE_OPERATING_SKILL_KEY
        );
        assert_eq!(
            main_skill_key_for_frozen_revision(LEGACY_MAIN_BASELINE_OPERATING_SKILL_REVISION),
            MAIN_BASELINE_OPERATING_SKILL_KEY
        );
        assert_eq!(
            main_skill_key_for_frozen_revision("forge.main.project-discovery/v2@7"),
            MAIN_OPERATING_SKILL_KEY
        );
    }

    #[test]
    fn cli_chat_wraps_profile_config_in_executor_snapshot() {
        let snapshot = cli_executor_snapshot(
            "smith",
            serde_json::json!({"profile": "luna", "approval": "deny"}),
        );
        assert_eq!(snapshot["executor_type"], "smith");
        assert_eq!(snapshot["config"]["profile"], "luna");
        assert_eq!(snapshot["config"]["approval"], "deny");
    }

    #[test]
    fn cli_chat_applies_frozen_profile_execution_fields() {
        let profile = AgentProfile {
            id: "profile-id".to_owned(),
            identity_id: "identity-id".to_owned(),
            backend_kind: "cli".to_owned(),
            executor_type: "smith".to_owned(),
            provider: Some("chatgpt".to_owned()),
            model: Some("gpt-5.6-terra".to_owned()),
            reasoning_effort: Some("high".to_owned()),
            permission_policy: Some("auto".to_owned()),
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: r#"{"profile":"luna","provider":"google","model":"gemini-3.6-flash"}"#
                .to_owned(),
            credential_ref: None,
            daemon_id: None,
            version: 1,
            created_at: "2026-08-26T00:00:00Z".to_owned(),
            updated_at: "2026-08-26T00:00:00Z".to_owned(),
        };

        let config = cli_profile_execution_config(&profile)
            .expect("frozen profile fields apply to the CLI config");

        assert_eq!(config["profile"], "luna");
        assert_eq!(config["provider"], "chatgpt");
        assert_eq!(config["model"], "gpt-5.6-terra");
        assert_eq!(config["model_reasoning_effort"], "high");
        assert_eq!(config["effort"], "high");
        assert_eq!(config["permission_policy"], "auto");
    }

    #[test]
    fn cli_assistant_output_is_bounded_before_persistence() {
        let content = cli_result_content(ExecutionResult {
            status: ExecutionOutcome::Completed,
            assistant_output: Some("x".repeat(MAX_CLI_ASSISTANT_CHARS + 100)),
            ..Default::default()
        })
        .expect("completed CLI output is admitted");
        assert_eq!(content.chars().count(), MAX_CLI_ASSISTANT_CHARS);
    }

    #[test]
    fn errors_are_bounded_and_classified_without_body_leak() {
        let error = ServiceError::invalid_operation("x".repeat(2048));
        assert_eq!(
            bounded_error_message(&error.to_string()).chars().count(),
            MAX_ERROR_CHARS
        );
        assert_eq!(
            classify_turn_error(&ServiceError::invalid_operation("credential unavailable")),
            "credential_unavailable"
        );
        assert_eq!(
            classify_turn_error(&ServiceError::invalid_operation(
                "Project Agent Charter pointer is stale"
            )),
            "authority_denied"
        );
    }

    #[test]
    fn server_owned_operating_instruction_is_added_to_both_backend_contexts() {
        let instruction = "Product Genesis protocol v1\nAsk at most two questions.";
        let system = compose_system_prompt(Some("profile rules"), Some(instruction))
            .expect("an active instruction produces system context");
        assert!(system.contains("profile rules"));
        assert!(system.contains("SERVER-OWNED OPERATING INSTRUCTION"));
        assert!(system.contains(instruction));

        let prompt = build_cli_prompt(
            Some("profile rules"),
            Some(instruction),
            &[],
            "continue discovery",
        );
        assert!(prompt.contains("SERVER-OWNED OPERATING INSTRUCTION"));
        assert!(prompt.contains("continue discovery"));
    }

    #[test]
    fn delivery_followup_retry_adds_server_owned_corrective_instruction() {
        let mut instruction = Some("Project operating instruction".to_owned());
        append_delivery_followup_retry_instruction(
            Some(DELIVERY_FOLLOWUP_POSTCONDITION_FAILED),
            true,
            &mut instruction,
        )
        .expect("delivery follow-up retry instruction");
        let instruction = instruction.expect("Project operating instruction remains available");
        assert!(instruction.contains("SERVER-OWNED DELIVERY FOLLOW-UP RETRY"));
        assert!(instruction.contains("invoke `project.readiness`"));
        assert!(instruction.contains("Narration cannot complete this turn"));

        let mut ordinary = Some("Project operating instruction".to_owned());
        append_delivery_followup_retry_instruction(Some("backend_failed"), true, &mut ordinary)
            .expect("ordinary retries are unchanged");
        assert_eq!(ordinary.as_deref(), Some("Project operating instruction"));
    }

    #[test]
    fn terminal_genesis_has_no_instruction_overlay() {
        assert!(compose_system_prompt(Some("profile rules"), None)
            .expect("profile prompt remains available")
            .contains("profile rules"));
        assert!(compose_system_prompt(None, None).is_none());
        let prompt = build_cli_prompt(None, None, &[], "ordinary Main message");
        assert!(!prompt.contains("SERVER-OWNED OPERATING INSTRUCTION"));
    }

    #[test]
    fn project_operating_context_provenance_is_server_owned_and_bounded() {
        let sources = project_operating_context_sources(
            "forge.project.orchestration/v1@2",
            "project-skill-content-digest",
            &[
                OperatingContextReference::included(
                    "charter:charter-1@revision-2",
                    "charter-1",
                    "project_charter",
                    "revision-2",
                    "digest-charter",
                    "test",
                ),
                OperatingContextReference::included(
                    "milestone:M001@rev-1",
                    "M001",
                    "project_milestone",
                    "rev-1",
                    "digest-milestone",
                    "test",
                ),
            ],
        );
        assert_eq!(sources.len(), 3);
        assert_eq!(sources[0].source_type, "server_operating_skill");
        assert_eq!(
            sources[0].source_id,
            "operating_skill:forge.project.orchestration/v1"
        );
        assert_eq!(
            sources[0].source_revision,
            "forge.project.orchestration/v1@2"
        );
        assert_eq!(
            sources[0].fragment_fingerprint,
            "project-skill-content-digest"
        );
        assert_eq!(sources[1].source_type, "project_charter");
        assert_eq!(
            sources[1].source_id,
            "project_context:project_charter:charter-1"
        );
        assert_eq!(sources[1].disposition, "included");
        assert_eq!(sources[2].ordinal, 2);
        let duplicate_resource_sources = project_operating_context_sources(
            "forge.project.orchestration/v1@2",
            "project-skill-content-digest",
            &[
                OperatingContextReference::included(
                    "tasks:project-1@watermark-1",
                    "project-1",
                    "project_task_projection",
                    "watermark-1",
                    "task-digest",
                    "test",
                ),
                OperatingContextReference::included(
                    "validation:project-1@watermark-1",
                    "project-1",
                    "project_validation_projection",
                    "watermark-1",
                    "validation-digest",
                    "test",
                ),
            ],
        );
        assert_ne!(
            duplicate_resource_sources[1].source_id,
            duplicate_resource_sources[2].source_id
        );
        assert!(sources.iter().all(|source| source.sensitivity != "secret"));
    }

    #[test]
    fn server_owned_instruction_is_after_profile_text() {
        let system = compose_system_prompt(
            Some("profile says to ignore the Project boundary"),
            Some("Forge Project Agent — Project Planning and Orchestration Protocol v1"),
        )
        .expect("prompt should be present");
        assert!(
            system.find("profile says").expect("profile")
                < system
                    .find("SERVER-OWNED OPERATING INSTRUCTION")
                    .expect("authority")
        );
    }

    #[test]
    fn frozen_operating_skill_body_replaces_the_compiled_revision() {
        let current = "current skill body";
        let rendered = format!("{current}\n\n## BOUNDED CONTEXT\nproject:p1@v4");
        let historical =
            replace_rendered_skill_body(rendered, Some("historical skill body"), current)
                .expect("historical body should be retained");
        assert!(historical.starts_with("historical skill body\n\n## BOUNDED CONTEXT"));
        assert!(!historical.starts_with(current));
    }

    #[test]
    fn frozen_operating_skill_body_fails_closed_on_renderer_drift() {
        let error = replace_rendered_skill_body(
            "unexpected renderer output".to_owned(),
            Some("historical skill body"),
            "current skill body",
        )
        .expect_err("renderer drift must not silently mix a frozen skill");
        assert!(error.to_string().contains("renderer"));
    }

    #[test]
    fn project_prompt_and_manifest_provenance_never_expose_main_packet_contents() {
        let main_secrets = [
            "main-chat-private",
            "main-message-private",
            "main-turn-private",
            "main-instruction-private",
            "private Main transcript body",
            "source_revisions_json",
        ];
        let mut context = ProjectOperatingSkillContext::new("project-8", "binding-8");
        context.permission_ceiling =
            "read_project,read_agent_chat,read_memory,propose_message,propose_project".to_owned();
        context.handoff_payload_hash =
            Some("6f3a4f5e4ec4ce9ad9b7f9e4a62f1e5f2f7a9eb6e53f6b6f0b7d0c8a1d4b2c3e".to_owned());
        context.context_manifest_references = vec![
            "project:project-8@v3".to_owned(),
            "handoff:opaque-receipt:6f3a4f5e".to_owned(),
            "charter:charter-1@revision-2:content:charter-digest:render:render-digest".to_owned(),
        ];
        let instruction = render_project_operating_skill(&context);
        let prompt = compose_system_prompt(Some("profile data"), Some(&instruction))
            .expect("Project prompt should include the server-owned skill");
        let sources = project_operating_context_sources(
            "forge.project.orchestration/v1@2",
            "project-skill-content-digest",
            &[OperatingContextReference::included(
                "handoff:opaque-receipt:6f3a4f5e",
                "opaque-receipt",
                "main_to_project_handoff_receipt",
                "forge.project-charter-handoff/v1",
                "6f3a4f5e",
                "opaque_handoff_provenance_only",
            )],
        );
        for secret in main_secrets {
            assert!(!prompt.contains(secret), "prompt leaked {secret}");
            assert!(
                sources
                    .iter()
                    .all(|source| !source.source_id.contains(secret)
                        && !source.source_revision.contains(secret)
                        && !source.fragment_fingerprint.contains(secret)),
                "manifest leaked {secret}"
            );
        }
        assert!(prompt.contains("Handoff payload hash"));
        assert!(prompt.contains("opaque-receipt:6f3a4f5e"));
        assert!(sources.iter().any(|source| {
            source.source_type == "main_to_project_handoff_receipt"
                && source.selection_reason == "opaque_handoff_provenance_only"
        }));
    }

    #[test]
    fn effective_state_context_keeps_digests_conflicts_and_unreleased_changes() {
        let projection: ProjectEffectiveStateProjection = serde_json::from_value(serde_json::json!({
            "project": {
                "id": "project-8", "name": "Project Eight", "paused": false,
                "charter_status": "charter_backed", "charter_setup_required": false,
                "version": 4, "created_at": "2026-08-13T00:00:00Z", "updated_at": "2026-08-13T00:00:01Z"
            },
            "governing_charter": {
                "id": "charter-1", "revision_id": "charter-revision-2", "revision": 2,
                "version": 3, "content_digest": "charter-content", "render_digest": "charter-render"
            },
            "active_execution_baseline": {
                "id": "baseline-1", "revision_id": "baseline-revision-2", "revision": 2,
                "version": 2, "lifecycle": "active", "charter_revision_id": "charter-revision-2",
                "content_digest": "baseline-content", "render_digest": "baseline-render",
                "release_policy_revision": "policy-2", "release_policy_digest": "policy-digest"
            },
            "approved_documents": [{
                "id": "document-1", "kind": "delivery_brief", "title": "Brief",
                "revision_id": "document-revision-3", "revision": 3, "version": 2,
                "lifecycle": "approved", "content_digest": "document-content", "render_digest": "document-render"
            }],
            "active_decisions": [{
                "id": "decision-1", "state": "active", "decision_class": "project_implementation",
                "question": "Which loop?", "selected_outcome": "Loop A", "rationale": "Smallest test",
                "principal_type": "agent", "principal_id": "identity-1",
                "authority_basis": "active_execution_baseline_adaptive_envelope",
                "authorization_action": "project.decision.record_effective",
                "explicit_event": "action-1", "authorization_occurred_at": "2026-08-13T00:00:02Z",
                "charter_revision_id": "charter-revision-2",
                "baseline_revision_id": "baseline-revision-2", "source_refs": [],
                "affected_records": {}, "created_at": "2026-08-13T00:00:02Z"
            }],
            "invalidated_decisions": [],
            "reconciliation_required": [{
                "id": "reconcile-1", "conflict_id": "conflict-1", "record_type": "task",
                "record_id": "task-1", "record_revision": "v3", "record_digest": "task-digest",
                "state": "required", "version": 2, "updated_at": "2026-08-13T00:00:03Z"
            }],
            "canonical_conflicts": [{
                "id": "conflict-1", "domain": "execution", "governing_record_type": "baseline",
                "governing_record_id": "baseline-1", "governing_record_revision": "baseline-revision-2",
                "governing_record_digest": "baseline-content", "conflicting_record_type": "task",
                "conflicting_record_id": "task-1", "conflicting_record_revision": "v3",
                "conflicting_record_digest": "task-digest", "affected_paths": ["scope"],
                "conflict_code": "stale_task", "description": "Task is outside baseline", "created_at": "2026-08-13T00:00:03Z"
            }],
            "task_summary": {"total": 2, "by_status": [{"key": "todo", "count": 2}]},
            "validation_summary": {"total": 1, "by_outcome": [{"key": "stale", "count": 1}]},
            "commitments": [{
                "id": "commitment-1", "status": "blocked", "due_at": null,
                "originating_task_id": "task-1", "evidence_required": true,
                "evidence_count": 0, "blocked_reason": "awaiting evidence",
                "version": 2, "updated_at": "2026-08-13T00:00:02Z"
            }],
            "inbox": [{
                "id": "inbox-1", "kind": "task_outcome", "status": "unread",
                "source_type": "task", "source_id": "task-1",
                "correlation_id": "corr-1", "version": 1,
                "created_at": "2026-08-13T00:00:02Z", "updated_at": "2026-08-13T00:00:03Z"
            }],
            "active_milestones": [{
                "id": "milestone-1", "milestone_key": "M001", "display_label": "Deliver",
                "lifecycle": "active", "definition_revision_id": "milestone-revision-2",
                "definition_digest": "milestone-content", "version": 2,
                "blocker_reasons": [], "stale_reasons": ["stale check"], "reconciliation_reasons": []
            }],
            "primary_milestone_id": "milestone-1",
            "readiness": {
                "latest": {
                    "id": "readiness-1", "milestone_id": "milestone-1", "definition_revision_id": "milestone-revision-2",
                    "baseline_id": "baseline-1", "baseline_revision_id": "baseline-revision-2",
                    "baseline_digest": "baseline-content", "release_policy_revision": "policy-2",
                    "release_policy_digest": "policy-digest", "event_watermark": "event-8",
                    "outcome": "blocked", "blocking_reasons": ["stale"], "readiness_digest": "readiness-digest",
                    "created_at": "2026-08-13T00:00:04Z"
                },
                "by_milestone": []
            },
            "releases": [{
                "id": "release-1", "milestone_id": "milestone-1", "release_sequence": 1,
                "release_revision": 1, "release_identifier": "M001-r1", "readiness_snapshot_id": "readiness-0",
                "readiness_digest": "readiness-0-digest", "baseline_id": "baseline-0",
                "baseline_revision_id": "baseline-revision-1", "baseline_digest": "baseline-0-content",
                "snapshot_digest": "release-snapshot-0-digest",
                "created_at": "2026-08-12T00:00:00Z"
            }],
            "unreleased_changes": {
                "document_ids": ["document-2"], "decision_candidate_ids": ["candidate-1"],
                "baseline_revision_ids": ["baseline-revision-3"], "active_milestone_ids": ["milestone-1"],
                "reconciliation_ids": ["reconcile-1"]
            },
            "source_event_watermark": "event-8", "source_event_sequence": 8,
            "source_project_version": 4, "source_project_work_epoch": 9
        })).expect("typed effective-state projection");
        let state = effective_state_context(&projection).expect("canonical effective state");
        assert!(state
            .governing_charter
            .expect("Charter")
            .contains("charter-content"));
        assert!(state
            .active_execution_baseline
            .expect("baseline")
            .contains("policy-2@policy-digest"));
        assert!(state.active_decisions[0].contains("digest:"));
        assert!(state.reconciliation_required[0].contains("task-digest"));
        assert!(state
            .reconciliation_required
            .iter()
            .any(|value| value.contains("commitment-1") && value.contains("inbox-1")));
        assert!(state.canonical_conflicts[0].contains("baseline-content"));
        assert!(state.task_summary.contains("total=2"));
        assert!(state.validation_summary.contains("total=1"));
        assert!(state.active_milestones[0].contains("milestone-content"));
        assert!(state.readiness.contains("event:event-8"));
        assert!(state.releases[0].contains("baseline-revision-1"));
        assert!(state
            .reconciliation_required
            .iter()
            .any(|value| value.contains("unreleased changes")));
        assert_eq!(state.event_watermark.as_deref(), Some("event-8"));
    }

    fn handoff_packet_json() -> String {
        let mut packet = serde_json::json!({
            "schema_version": PROJECT_HANDOFF_SCHEMA_VERSION,
            "handoff_id": "handoff-1",
            "deduplication_key": "dedupe-1",
            "correlation_id": "correlation-1",
            "causation_id": "event-1",
            "approval_id": "approval-3",
            "request": {
                "policy_revision": "forge.project-agent-policy/v1",
                "policy_digest": "policy-digest-7",
                "source_revisions_digest": "source-revisions-digest-1",
                "source_revisions_json": "{\"schema_version\":\"forge.project-charter-handoff/v1\"}",
                "authorization": {
                    "principal_type": "user",
                    "principal_id": "account-1",
                    "authorization_basis": "approved_charter",
                    "action": "create_project_from_charter_approval",
                    "event_id": "approval-event-4",
                    "occurred_at": "2026-08-13T00:00:00Z"
                }
            },
            "source": {
                "chat_id": "main-chat-1",
                "message_ids": ["message-1", "message-2"],
                "message_id": "message-2",
                "turn_id": "source-turn-1",
                "identity_id": "main-identity-1",
                "profile_revision_id": "main-profile-1",
                "instruction_revision_id": "instruction-1",
                "instruction_revision": 3
            },
            "project": {
                "id": "project-8",
                "name": "Project Eight",
                "lifecycle": "active",
                "mode": "standard",
                "approved_slug": "project-eight"
            },
            "target": {
                "chat_id": "project-chat-8",
                "binding_id": "binding-8",
                "identity_id": "identity-5",
                "profile_revision_id": "profile-6",
                "message_id": "target-message-8",
                "turn_id": "target-turn-8"
            },
            "charter": {
                "id": "charter-1",
                "revision_id": "revision-2",
                "revision_number": 2,
                "schema_version": "forge.project-charter/v1",
                "content_digest": "content-digest-2",
                "render_version": "forge-project-charter/v1",
                "render_digest": "render-digest-2"
            },
            "approval": {
                "id": "approval-3",
                "event_id": "approval-event-4",
                "authorization_basis": "approved_charter",
                "authorization_action": "project.charter.approve",
                "authorization_event_id": "charter-authorization-event-4",
                "authorization_occurred_at": "2026-08-12T23:59:59Z",
                "approved_by": {
                    "kind": "user",
                    "id": "account-1"
                },
                "approved_at": "2026-08-13T00:00:00Z"
            },
            "project_agent": {
                "identity_id": "identity-5",
                "profile_revision_id": "profile-6",
                "operating_skill_revision": "forge.project.orchestration/v1@2",
                "policy_revision": "forge.project-agent-policy/v1",
                "policy_digest": "policy-digest-7"
            },
            "bounded_summary": "Approved Project handoff summary.",
            "settled_decision_ids": ["decision-1"],
            "unresolved_items": [{"id": "unresolved-1", "question": "Which low-risk option should be tested?"}],
            "research_references": [{"url": "https://example.test/reference", "claim": "bounded public reference"}],
            "content_classification": "approved_project_charter",
            "redaction_manifest": {
                "excluded_knowledge_item_ids": ["memory-1"],
                "excluded_categories": [
                    "full_main_chat_history",
                    "hidden_memory_bodies",
                    "credentials",
                    "protected_runtime_or_browser_state",
                    "unrelated_projects",
                    "authority_bearing_text"
                ]
            },
            "created_at": "2026-08-13T00:00:00Z",
            "delivery": {
                "delivered_at": "2026-08-13T00:00:01Z"
            }
        });
        let authorization = ProjectCharterHandoffAuthorization {
            principal_type: "user".to_owned(),
            principal_id: "account-1".to_owned(),
            authorization_basis: "create_project_from_approved_charter".to_owned(),
            action: "create_project_from_charter_approval".to_owned(),
            event_id: "approval-event-4".to_owned(),
            occurred_at: "2026-08-13T00:00:00Z".to_owned(),
        };
        packet["request"]["authorization"] = serde_json::to_value(&authorization).expect("auth");
        let mut source_manifest = packet.clone();
        source_manifest
            .as_object_mut()
            .expect("source manifest object")
            .remove("request");
        source_manifest
            .as_object_mut()
            .expect("source manifest object")
            .remove("approval_id");
        source_manifest["target"]["chat_id"] = Value::Null;
        source_manifest["source"]
            .as_object_mut()
            .expect("source object")
            .remove("message_id");
        source_manifest["delivery"]["delivered_at"] = Value::Null;
        packet["request"]["source_revisions_json"] = Value::String(source_manifest.to_string());
        packet["request"]["source_revisions_digest"] =
            Value::String(handoff_request_fingerprint(&packet, &authorization).expect("digest"));
        packet.to_string()
    }

    fn handoff_expectation<'a>() -> ProjectHandoffExpectation<'a> {
        ProjectHandoffExpectation {
            handoff_id: "handoff-1",
            deduplication_key: "dedupe-1",
            correlation_id: "correlation-1",
            causation_id: "event-1",
            source_chat_id: "main-chat-1",
            source_identity_id: "main-identity-1",
            source_profile_revision_id: "main-profile-1",
            source_instruction_revision_id: "instruction-1",
            source_instruction_revision: 3,
            source_message_ids: vec!["message-1".to_owned(), "message-2".to_owned()],
            source_turn_id: Some("source-turn-1"),
            project_id: "project-8",
            project_name: "Project Eight",
            project_mode: "standard",
            approved_slug: Some("project-eight"),
            target_chat_id: "project-chat-8",
            target_binding_id: "binding-8",
            target_message_id: "target-message-8",
            target_turn_id: "target-turn-8",
            charter_id: "charter-1",
            charter_revision_id: "revision-2",
            charter_revision_number: 2,
            charter_schema_version: "forge.project-charter/v1",
            charter_content_digest: "content-digest-2",
            charter_render_version: "forge-project-charter/v1",
            charter_render_digest: "render-digest-2",
            approval_id: "approval-3",
            approval_event_id: "approval-event-4",
            approval_authorization_basis: "approved_charter",
            approval_authorization_action: "project.charter.approve",
            approval_authorization_event_id: "charter-authorization-event-4",
            approval_authorization_occurred_at: "2026-08-12T23:59:59Z",
            approval_principal_kind: "user",
            approval_principal_id: "account-1",
            approval_created_at: "2026-08-13T00:00:00Z",
            create_authorization_principal_type: "user",
            create_authorization_principal_id: "account-1",
            create_authorization_basis: "create_project_from_approved_charter",
            create_authorization_action: "create_project_from_charter_approval",
            create_authorization_event_id: "approval-event-4",
            create_authorization_occurred_at: "2026-08-13T00:00:00Z",
            identity_id: "identity-5",
            profile_revision_id: "profile-6",
            policy_revision: "forge.project-agent-policy/v1",
            policy_digest: "policy-digest-7",
            created_at: "2026-08-13T00:00:00Z",
            delivered_at: "2026-08-13T00:00:01Z",
        }
    }

    #[test]
    fn project_handoff_accepts_the_full_approved_packet_shape() {
        let packet = validate_project_handoff_packet(
            &handoff_packet_json(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect("the full server-owned packet should parse and authenticate");
        assert_eq!(packet.handoff_id, "handoff-1");
        assert_eq!(packet.target.chat_id, "project-chat-8");
        assert_eq!(packet.redaction_manifest.excluded_categories.len(), 6);
    }

    #[test]
    fn project_handoff_fingerprint_matches_database_materialized_transport_fields() {
        let packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        let authorization: ProjectCharterHandoffAuthorization =
            serde_json::from_value(packet["request"]["authorization"].clone())
                .expect("authorization");
        let expected = packet["request"]["source_revisions_digest"]
            .as_str()
            .expect("source digest");

        let mut candidate = packet.clone();
        candidate["handoff_id"] = Value::String("handoff-allocated-later".to_owned());
        candidate["correlation_id"] = Value::String("correlation-allocated-later".to_owned());
        candidate["created_at"] = Value::String("2026-08-14T00:00:00Z".to_owned());
        candidate["project"]["id"] = Value::String("project-allocated-later".to_owned());
        candidate["target"]["chat_id"] = Value::String("chat-allocated-later".to_owned());
        candidate["target"]["binding_id"] = Value::String("binding-allocated-later".to_owned());
        candidate["target"]["message_id"] = Value::String("message-allocated-later".to_owned());
        candidate["target"]["turn_id"] = Value::String("turn-allocated-later".to_owned());
        candidate["source"]["message_id"] = Value::String("source-allocated-later".to_owned());
        candidate["delivery"]["delivered_at"] = Value::String("2026-08-14T00:00:01Z".to_owned());

        let mut nested: Value = serde_json::from_str(
            candidate["request"]["source_revisions_json"]
                .as_str()
                .expect("source manifest"),
        )
        .expect("source manifest JSON");
        nested["handoff_id"] = Value::String("nested-handoff-allocated-later".to_owned());
        nested["correlation_id"] = Value::String("nested-correlation-allocated-later".to_owned());
        nested["created_at"] = Value::String("2026-08-14T00:00:02Z".to_owned());
        nested["project"]["id"] = Value::String("nested-project-allocated-later".to_owned());
        nested["target"]["binding_id"] = Value::String("nested-binding-allocated-later".to_owned());
        nested["target"]["message_id"] = Value::String("nested-message-allocated-later".to_owned());
        nested["target"]["turn_id"] = Value::String("nested-turn-allocated-later".to_owned());
        candidate["request"]["source_revisions_json"] = Value::String(nested.to_string());

        assert_eq!(
            expected,
            handoff_request_fingerprint(&candidate, &authorization)
                .expect("database transport allocations are excluded")
        );
    }

    #[test]
    fn project_handoff_canonicalization_is_stable_for_nested_object_order() {
        let left = serde_json::json!({
            "outer": {"z": 1, "a": [{"right": true, "left": false}]},
            "first": "value"
        });
        let right = serde_json::json!({
            "first": "value",
            "outer": {"a": [{"left": false, "right": true}], "z": 1}
        });

        assert_eq!(
            canonicalize_project_handoff_json(&left),
            canonicalize_project_handoff_json(&right)
        );
        assert_eq!(
            serde_json::to_string(&canonicalize_project_handoff_json(&left)).expect("canonical"),
            serde_json::to_string(&canonicalize_project_handoff_json(&right)).expect("canonical")
        );
    }

    #[test]
    fn project_handoff_rejects_mismatched_charter_revision() {
        let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        packet["charter"]["revision_id"] = Value::String("revision-stale".to_owned());
        let error = validate_project_handoff_packet(
            &packet.to_string(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect_err("stale Charter revision must fail closed");
        assert!(error.to_string().contains("Project Agent handoff"));
    }

    #[test]
    fn project_handoff_rejects_mismatched_policy_digest() {
        let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        packet["project_agent"]["policy_digest"] = Value::String("policy-stale".to_owned());
        let error = validate_project_handoff_packet(
            &packet.to_string(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect_err("stale policy digest must fail closed");
        assert!(error.to_string().contains("Project Agent handoff"));
    }

    #[test]
    fn project_handoff_rejects_tampered_source_manifest_digest() {
        let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        packet["request"]["source_revisions_json"] =
            Value::String("{\"schema_version\":\"tampered\"}".to_owned());
        let error = validate_project_handoff_packet(
            &packet.to_string(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect_err("a source-manifest mutation must invalidate its digest");
        assert!(error.to_string().contains("Project Agent handoff"));
    }

    #[test]
    fn project_handoff_rejects_semantically_tampered_source_manifest_even_with_new_digest() {
        let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        let mut source_manifest: Value = serde_json::from_str(
            packet["request"]["source_revisions_json"]
                .as_str()
                .expect("source manifest"),
        )
        .expect("source manifest JSON");
        source_manifest["bounded_summary"] = Value::String("tampered summary".to_owned());
        packet["request"]["source_revisions_json"] = Value::String(source_manifest.to_string());
        let authorization: ProjectCharterHandoffAuthorization =
            serde_json::from_value(packet["request"]["authorization"].clone())
                .expect("authorization");
        packet["request"]["source_revisions_digest"] =
            Value::String(handoff_request_fingerprint(&packet, &authorization).expect("digest"));
        let error = validate_project_handoff_packet(
            &packet.to_string(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect_err("a semantically altered source manifest must fail closed");
        assert!(error.to_string().contains("Project Agent handoff"));
    }

    #[test]
    fn project_handoff_rejects_tampered_approval_authorization() {
        let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        packet["approval"]["authorization_action"] =
            Value::String("project.charter.revoke".to_owned());
        let error = validate_project_handoff_packet(
            &packet.to_string(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect_err("approval authorization changes must fail closed");
        assert!(error.to_string().contains("Project Agent handoff"));
    }

    #[test]
    fn project_handoff_rejects_tampered_create_authorization_even_with_new_digest() {
        let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        packet["request"]["authorization"]["action"] =
            Value::String("project.handoff.replay".to_owned());
        let authorization: ProjectCharterHandoffAuthorization =
            serde_json::from_value(packet["request"]["authorization"].clone())
                .expect("authorization");
        packet["request"]["source_revisions_digest"] =
            Value::String(handoff_request_fingerprint(&packet, &authorization).expect("digest"));
        let error = validate_project_handoff_packet(
            &packet.to_string(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect_err("create authorization changes must fail closed");
        assert!(error.to_string().contains("Project Agent handoff"));
    }

    #[test]
    fn project_handoff_rejects_tampered_delivery_and_redaction_shape() {
        let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        packet["delivery"]["delivered_at"] = Value::String("delivery-stale".to_owned());
        packet["redaction_manifest"]["excluded_categories"] =
            serde_json::json!(["full_main_chat_history"]);
        let error = validate_project_handoff_packet(
            &packet.to_string(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect_err("delivery and redaction provenance must fail closed");
        assert!(error.to_string().contains("Project Agent handoff"));
    }

    #[test]
    fn project_handoff_rejects_unknown_fields_without_discarding_them() {
        let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
        packet["source"]["untrusted_extra"] = Value::String("must reject".to_owned());
        let error = validate_project_handoff_packet(
            &packet.to_string(),
            &handoff_expectation(),
            "consumed",
            Some("project-8"),
            "project-8",
        )
        .expect_err("unknown packet fields must not be discarded");
        assert!(error.to_string().contains("invalid typed Charter packet"));
    }

    #[test]
    fn project_handoff_requires_the_v076_approval_and_request_envelope() {
        for field in ["approval_id", "request"] {
            let mut packet: Value = serde_json::from_str(&handoff_packet_json()).expect("packet");
            packet.as_object_mut().expect("object").remove(field);
            let error = validate_project_handoff_packet(
                &packet.to_string(),
                &handoff_expectation(),
                "consumed",
                Some("project-8"),
                "project-8",
            )
            .expect_err("required V076 handoff fields must not be optional");
            assert!(error.to_string().contains("invalid typed Charter packet"));
        }
    }

    #[test]
    fn legacy_setup_instruction_only_allows_read_and_adoption_proposals() {
        assert!(PROJECT_SETUP_RESTRICTIONS.contains("propose an adoption Charter"));
        assert!(
            PROJECT_SETUP_RESTRICTIONS.contains("Do not create or dispatch implementation Tasks")
        );
        assert!(PROJECT_SETUP_RESTRICTIONS.contains("repository Workspace"));
        assert_eq!(
            PROJECT_SETUP_PERMISSION_CEILING,
            "read_project,read_agent_chat,read_memory,propose_message,propose_project"
        );
        assert!(!PROJECT_SETUP_PERMISSION_CEILING.contains("propose_task"));
    }

    #[test]
    fn main_genesis_context_provenance_names_skill_instruction_and_charter() {
        let sources = main_operating_context_sources(
            MAIN_OPERATING_SKILL_KEY,
            "forge.main.project-discovery/v2@1",
            "main-skill-content-digest",
            "server_owned_main_genesis_operating_skill",
            "main_genesis_context",
            &[
                OperatingContextReference::included(
                    "genesis:genesis-1@v2",
                    "genesis-1",
                    "main_genesis",
                    "v2",
                    "genesis-digest",
                    "test",
                ),
                OperatingContextReference::included(
                    "main_instruction:instruction-1@3",
                    "instruction-1",
                    "main_genesis_instruction",
                    "3",
                    "instruction-digest",
                    "test",
                ),
                OperatingContextReference::included(
                    "charter:charter-1@revision-2",
                    "charter-1",
                    "main_charter",
                    "revision-2",
                    "charter-digest",
                    "test",
                ),
                OperatingContextReference::included(
                    "portfolio:project-1@v4",
                    "project-1",
                    "main_portfolio_projection",
                    "v4:2026-08-13T00:00:00Z",
                    "portfolio-digest",
                    "bounded_account_portfolio_projection",
                ),
            ],
        );
        assert_eq!(
            sources[0].source_id,
            "operating_skill:forge.main.project-discovery/v2"
        );
        assert_eq!(
            sources[0].source_revision,
            "forge.main.project-discovery/v2@1"
        );
        assert_eq!(sources[0].fragment_fingerprint, "main-skill-content-digest");
        assert!(sources
            .iter()
            .any(|source| source.source_type == "main_genesis_instruction"));
        assert!(sources.iter().any(|source| {
            source.source_type == "main_portfolio_projection"
                && source.disposition == "included"
                && source.sensitivity != "secret"
        }));
    }

    #[test]
    fn main_baseline_context_provenance_names_skill_profile_and_portfolio() {
        let sources = main_operating_context_sources(
            MAIN_BASELINE_OPERATING_SKILL_KEY,
            MAIN_BASELINE_OPERATING_SKILL_REVISION,
            MAIN_BASELINE_OPERATING_SKILL_CONTENT_DIGEST,
            "server_owned_main_baseline_operating_skill",
            "main_baseline_context",
            &[
                OperatingContextReference::included(
                    "main_profile:profile-1@v2",
                    "profile-1",
                    "main_profile",
                    "v2",
                    "profile-digest",
                    "authenticated_main_profile",
                ),
                OperatingContextReference::included(
                    "portfolio:project-1@v4",
                    "project-1",
                    "main_portfolio_projection",
                    "v4:2026-08-13T00:00:00Z",
                    "portfolio-digest",
                    "bounded_account_portfolio_projection",
                ),
            ],
        );
        assert_eq!(
            sources[0].source_id,
            "operating_skill:forge.main.baseline/v1"
        );
        assert_eq!(
            sources[0].source_revision,
            MAIN_BASELINE_OPERATING_SKILL_REVISION
        );
        assert_eq!(
            sources[0].selection_reason,
            "server_owned_main_baseline_operating_skill"
        );
        assert_eq!(
            sources[1].source_id,
            "main_baseline_context:main_profile:profile-1"
        );
        assert!(sources.iter().any(|source| {
            source.source_type == "main_portfolio_projection" && source.disposition == "included"
        }));
    }

    #[test]
    fn server_context_manifest_identity_is_stable_per_turn_job() {
        let first = agent_chat_server_manifest_id("identity-1", "session-1", "job-1");
        let replay = agent_chat_server_manifest_id("identity-1", "session-1", "job-1");
        let next_job = agent_chat_server_manifest_id("identity-1", "session-1", "job-2");
        let other_session = agent_chat_server_manifest_id("identity-1", "session-2", "job-1");

        assert_eq!(first, replay);
        assert_ne!(first, next_job);
        assert_ne!(first, other_session);
    }

    #[test]
    fn server_context_manifest_fingerprint_shape_excludes_context_bodies() {
        let source = ContextSourceInput {
            ordinal: 0,
            source_id: "main_genesis_context:main_genesis:genesis-1@v2".to_owned(),
            source_type: "main_genesis".to_owned(),
            source_revision: "v2:discovering".to_owned(),
            selection_reason: "active_product_genesis".to_owned(),
            disposition: "included".to_owned(),
            retention_priority: 100,
            fragment_fingerprint: "digest-1".to_owned(),
            sensitivity: "internal".to_owned(),
        };
        let encoded = serde_json::to_string(&canonical_operating_context_sources(&[source]))
            .expect("canonical source metadata serializes");

        assert!(encoded.contains("main_genesis_context:main_genesis:genesis-1@v2"));
        assert!(encoded.contains("digest-1"));
        assert!(!encoded.contains("body"));
        assert!(!encoded.contains("private transcript"));
    }

    #[test]
    fn project_handoff_rejects_active_approval() {
        let error = validate_project_handoff_packet(
            &handoff_packet_json(),
            &handoff_expectation(),
            "active",
            None,
            "project-8",
        )
        .expect_err("an active approval must not admit a Project turn");
        assert!(error.to_string().contains("consumed Charter approval"));
    }
}
