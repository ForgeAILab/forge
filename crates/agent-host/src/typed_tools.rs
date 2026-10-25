//! Scope-derived tools for Forge-hosted Agent Runtime and CLI Chat sessions.
//!
//! The runtime deliberately knows nothing about Forge identities, Projects,
//! Agent Chats, or Task roles. This module is the narrow host-owned composition
//! boundary: it turns one server-authorized canonical scope into an exact tool
//! registry and an authoritative security check.  Callers never provide the
//! actor or scope as tool arguments; those values are captured when the host
//! composes the tools.

#[cfg(test)]
use crate::{PROJECT_CURRENT_STATE_OPERATION, TASK_PLAN_OPERATION};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::ErrorKind,
    path::{Component, Path},
    process::Stdio,
    sync::{Arc, Mutex},
};

use agent_runtime::core::{
    cancel::Cancellation,
    clock::{Clock, SystemClock},
    grant::{
        GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckMode, SecurityCheckOutcome,
        SecurityCheckRevision,
    },
    ids::{RequestId, SessionId, TenantId, TurnId},
    prelude::{
        ActionClass, AuthorizationRequest, Deadline, DecisionCode, InteractionOrigin,
        InteractionRequest, InteractionResponse, InvocationContext, PermissionSet,
        PreparationContext, PreparedToolCall, RuntimeError, SecurityResource, Tool,
        ToolCallDisplay, ToolCallId, ToolEffects, ToolOutcome, ToolSpec,
    },
    security::{
        CheckSetRevision, SecurityAction, SecurityContext, SecurityEvidence, SecuritySubject,
    },
    workspace::{DenyAllWorkspace, Workspace},
};
use agent_runtime::registry::{Permission, TrustClass};
use agent_runtime::runtime::RuntimeBuilder;
use api_types::{DeniedBy, OrchestrationOutcome, OutcomeCode, RetryAction};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tokio::process::Command;

use agent_runtime::harness::{FetchTool, FetchTransport};

use crate::{
    AgentHostError, CanonicalScope, CanonicalScopeType, CommandAllowlist, WorkspaceAccess,
    operation_catalog::{
        MAIN_CHARTER_DRAFT_OPERATION, MAIN_INQUIRY_RUN_OPERATION, OperationExposure,
        OperationSurface, PROJECT_CHARTER_ADOPTION_OPERATION, operation_names_for_surface,
    },
    operation_contract::{
        coordination_payload_guidance, coordination_payload_properties,
        orchestration_proposal_schema, orchestration_read_schema, portable_const_schema,
        string_or_null_schema, validate_orchestration_proposal_arguments,
    },
};

/// Host-defined permission for a read-only Forge domain operation.
pub const FORGE_SCOPE_READ_PERMISSION: &str = "forge.scope.read";
/// Host-defined permission for a Forge proposal envelope.
pub const FORGE_SCOPE_PROPOSE_PERMISSION: &str = "forge.scope.propose";
/// Stable native tool name for bounded public web research.
pub const FORGE_PUBLIC_WEB_SEARCH_TOOL: &str = "forge_public_web_search";
/// Stable native tool name for bounded Task Workspace enumeration.
pub const FORGE_TASK_LIST_TOOL: &str = "forge_task_list";

/// Stable native tool name for Main Agent orchestration reads.
pub const FORGE_MAIN_ORCHESTRATION_READ_TOOL: &str = "forge_main_orchestration_read";
/// Stable native tool name for Main Agent orchestration proposals.
pub const FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL: &str = "forge_main_orchestration_propose";
/// Stable native tool name for Project Agent orchestration reads.
pub const FORGE_PROJECT_ORCHESTRATION_READ_TOOL: &str = "forge_project_orchestration_read";
/// Stable native tool name for Project Agent orchestration proposals.
pub const FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL: &str = "forge_project_orchestration_propose";

const MAX_DIRECTORY_ENTRIES: usize = 256;
const MAX_DIRECTORY_GLOB_CHARS: usize = 256;

/// The server-derived role of a public search invocation.  An Agent Chat's
/// opaque chat id is never enough to choose this value; native composition
/// receives the Project-chat bit only after Forge verifies its binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicSearchScope {
    Main,
    Project,
}

/// A provider for Forge domain reads and proposal envelopes.
///
/// The host resolves the identity and canonical scope from the persisted
/// Forge session before it constructs the tools.  Implementations therefore
/// receive server-derived values and must not accept replacement identity or
/// scope values from the model arguments.
/// One command a Project Agent ran in its verification checkout, as Forge
/// observed it: what ran, how it ended, and a digest of everything it printed.
/// The provider persists it and returns the observation id the Agent must cite
/// when it records a `task_validation` result, so a settled check always
/// points at something the Agent actually executed rather than something a
/// Task reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandObservation {
    pub session_id: String,
    pub turn_id: Option<String>,
    pub program: String,
    pub args: Vec<String>,
    pub exit_code: Option<i32>,
    pub success: bool,
    /// SHA-256 over the complete stdout, a separator, and the complete stderr.
    pub output_digest: String,
    pub stdout_excerpt: String,
    pub stderr_excerpt: String,
}

#[async_trait]
pub trait ForgeToolProvider: Send + Sync + fmt::Debug {
    fn admitted_authority(&self) -> Option<&operation_registry::authority::EffectiveAuthority> {
        None
    }
    async fn read_denial(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        let _ = (actor_identity_id, scope, operation);
        Ok(())
    }
    /// Persist a safe terminal-denial reminder using host-issued session and
    /// scope values. Inspection-only providers need no durable storage.
    async fn record_terminal_denial(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        session_token: &str,
        operation: &str,
        denied_by: &DeniedBy,
    ) -> Result<(), AgentHostError> {
        let _ = (
            actor_identity_id,
            scope,
            session_token,
            operation,
            denied_by,
        );
        Ok(())
    }

    /// Remove session reminders after a successful evaluation of the operation.
    async fn clear_terminal_denials(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        session_token: &str,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        let _ = (actor_identity_id, scope, session_token, operation);
        Ok(())
    }

    /// Performs one already-scope-bound, read-only domain operation.
    async fn read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError>;

    /// A registered query evaluates the authority pinned at turn admission.
    /// No fresh effect occurs, so the inner provider must not resolve again.
    async fn read_admitted(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
        authority: &operation_registry::authority::EffectiveAuthority,
    ) -> Result<Value, AgentHostError> {
        let _ = authority;
        self.read(actor, scope, operation, arguments).await
    }

    /// Persists one already-scope-bound proposal envelope.  The provider is
    /// responsible for applying Forge's policy intersection and for keeping
    /// proposals separate from authoritative Task/workflow mutation.
    /// `runtime_session_id` comes from the invocation context, never model
    /// arguments, so execution-scoped commands can bind to the exact
    /// protected session that invoked them.
    async fn propose(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError>;

    /// Retrieve an exact committed response without authorizing a fresh effect.
    /// The domain service must verify principal, scope, operation, key and digest.
    async fn replay_proposal(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: &Value,
    ) -> Result<Option<Value>, AgentHostError> {
        let _ = (actor, scope, operation, arguments);
        Ok(None)
    }

    /// Check current Main proposal authority before revealing payload contracts.
    async fn proposal_denial(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        let _ = (actor_identity_id, scope, operation);
        Ok(())
    }

    /// Execute a fingerprinted preparation, including one restored from storage.
    /// Its arguments have already been admitted; do not validate them against a
    /// newer contract. Implementations still enforce current authority/domain rules.
    async fn propose_prepared(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.propose(
            actor_identity_id,
            scope,
            runtime_session_id,
            operation,
            arguments,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn propose_admitted(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        arguments: Value,
        authority: &operation_registry::authority::EffectiveAuthority,
        prepared: bool,
    ) -> Result<Value, AgentHostError> {
        let _ = authority;
        if prepared {
            self.propose_prepared(
                actor_identity_id,
                scope,
                runtime_session_id,
                operation,
                arguments,
            )
            .await
        } else {
            self.propose(
                actor_identity_id,
                scope,
                runtime_session_id,
                operation,
                arguments,
            )
            .await
        }
    }

    /// Records one command a Project Agent verification session ran and
    /// returns `{"observation_id": ...}`. Surfaces without a verification
    /// workspace never compose the tool that calls this.
    async fn observe_command(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        observation: CommandObservation,
    ) -> Result<Value, AgentHostError> {
        let _ = (actor_identity_id, scope, observation);
        Err(AgentHostError::Unsupported(
            "this Forge surface records no command observations".to_owned(),
        ))
    }

    /// Returns whether a configured, unauthenticated public search endpoint
    /// is available.  The host uses this synchronous check to omit the tool
    /// entirely when search is not configured.
    fn public_search_configured(&self) -> bool {
        false
    }

    /// Executes one bounded public search.  The provider must derive and
    /// re-authorize the supplied scope before performing network I/O, and
    /// must return source metadata only (never persistence or approval).
    async fn public_search(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        search_scope: PublicSearchScope,
        query: &str,
        limit: u64,
    ) -> Result<Value, AgentHostError> {
        let _ = (actor_identity_id, scope, search_scope, query, limit);
        Err(AgentHostError::Unsupported(
            "public web search is not configured".to_owned(),
        ))
    }
}

/// The role admitted for a Task-scoped native session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskToolRole {
    /// May use bounded worktree reads, writes, and commands.
    Worker,
    /// May read the worktree and run the fixed validation check only.
    Reviewer,
    /// May read the worktree only; plans are authored through Task
    /// metadata/native domain tools, never through worktree writes.
    Planner,
}

impl TaskToolRole {
    fn parse(scope: &CanonicalScope, role: Option<&str>) -> Result<Self, AgentHostError> {
        match (scope.workspace_access, role) {
            (WorkspaceAccess::TaskWrite, Some("worker" | "coder")) => Ok(Self::Worker),
            (WorkspaceAccess::TaskRead, Some("reviewer")) => Ok(Self::Reviewer),
            (WorkspaceAccess::TaskRead, Some("planner")) => Ok(Self::Planner),
            // A write-capable role bound to a server-issued read-only Task
            // workspace (planning/discovery records dispatch their workflow's
            // coder role but never receive write authority) composes the
            // read-only planner toolset instead of being rejected.
            (WorkspaceAccess::TaskRead, Some("worker" | "coder")) => Ok(Self::Planner),
            (WorkspaceAccess::TaskWrite, Some(other))
            | (WorkspaceAccess::TaskRead, Some(other)) => Err(AgentHostError::Authority(format!(
                "Task workspace access is not valid for role `{other}`"
            ))),
            (_, None) => Err(AgentHostError::Authority(
                "Task tool composition requires a server-issued Task role".to_owned(),
            )),
            (
                WorkspaceAccess::Deny
                | WorkspaceAccess::ProjectVerify
                | WorkspaceAccess::AccountScratch,
                _,
            ) => Err(AgentHostError::Authority(
                "Task tool composition requires TaskRead or TaskWrite access".to_owned(),
            )),
        }
    }
}

/// An immutable, scope-derived native tool composition.
#[derive(Clone)]
pub struct ScopeToolComposition {
    tools: Vec<Arc<dyn Tool>>,
    security_check: Arc<dyn SecurityCheck>,
    coverage: PermissionSet,
    actor_identity_id: String,
    scope: CanonicalScope,
}

/// Protected Project Chat state used to derive the exact native tool catalog.
/// Forge resolves both fields from canonical server records before runtime
/// composition; neither is accepted from model tool arguments.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectChatToolContext {
    pub is_project_agent_chat: bool,
    pub charter_setup_required: bool,
}

/// Host-owned runtime inputs for one composition.
///
/// These are resolved by the server for this turn — never by model input —
/// and are optional so a composition built for inspection still behaves like
/// the real one, minus the surfaces that need a live transport.
#[derive(Clone, Default)]
pub struct ScopeToolRuntime {
    /// Programs workspace commands may spawn. `None` uses the built-in set.
    pub command_allowlist: Option<Arc<CommandAllowlist>>,
    pub environment: std::collections::BTreeMap<String, String>,
    /// Outbound transport for the runtime's web fetch tool. `None` leaves
    /// `fetch` out of the catalog entirely, which is what an inspection-only
    /// composition wants.
    pub fetch_transport: Option<Arc<dyn FetchTransport>>,
}

impl fmt::Debug for ScopeToolRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopeToolRuntime")
            .field(
                "command_allowlist",
                &self.command_allowlist.as_ref().map(|list| list.len()),
            )
            .field("fetch_transport", &self.fetch_transport.is_some())
            .finish()
    }
}

impl fmt::Debug for ScopeToolComposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopeToolComposition")
            .field("actor_identity_id", &self.actor_identity_id)
            .field("scope", &self.scope)
            .field("tools", &self.tool_names())
            .finish_non_exhaustive()
    }
}

mod composition;
mod result_handling;
mod scope_tools;
mod workspace_tools;

pub(crate) use result_handling::ToolResultFilter;
pub use result_handling::ToolResultObserver;
use result_handling::{FilteredTool, ObservedTool, TerminalDenialTool};
use scope_tools::{
    ForgePublicWebSearchTool, ForgeScopeProposeTool, ForgeScopeReadTool, ForgeScopeSecurityCheck,
};
pub use workspace_tools::PROJECT_VERIFICATION_CHECKOUT_DIR;
#[cfg(test)]
use workspace_tools::execute_workspace_command;
use workspace_tools::{
    CommandObserver, TaskCommandTool, TaskListTool, TaskReadTool, TaskValidateTool, TaskWriteTool,
};

fn non_task_operations(
    scope_type: CanonicalScopeType,
    project_agent_chat: bool,
    project_charter_setup_required: bool,
) -> (Vec<String>, Vec<String>) {
    let project_chat = ProjectChatToolContext {
        is_project_agent_chat: project_agent_chat,
        charter_setup_required: project_charter_setup_required,
    };
    let permissions = operation_registry::READ_CATALOG
        .iter()
        .chain(operation_registry::PROPOSAL_CATALOG.iter())
        .flat_map(|spec| {
            spec.authority
                .permissions
                .iter()
                .map(|(_, permission)| (*permission).to_owned())
        })
        .collect();
    if scope_type == CanonicalScopeType::Task {
        return (Vec::new(), Vec::new());
    }
    let names = |catalog: &operation_registry::OperationCatalog<std::convert::Infallible>,
                 aggregate| {
        catalog
            .iter()
            .filter(|spec| {
                spec.surfaces
                    .iter()
                    .any(|surface| surface.native_aggregate == aggregate)
            })
            .map(|spec| spec.id.to_owned())
            .collect::<Vec<_>>()
    };
    (
        filter_operations(
            scope_type,
            &names(&operation_registry::READ_CATALOG, "forge_scope_read"),
            &permissions,
            project_chat,
            None,
        ),
        filter_operations(
            scope_type,
            &names(&operation_registry::PROPOSAL_CATALOG, "forge_scope_propose"),
            &permissions,
            project_chat,
            None,
        ),
    )
}

fn public_search_scope(
    scope_type: CanonicalScopeType,
    project_agent_chat: bool,
) -> Option<PublicSearchScope> {
    match scope_type {
        CanonicalScopeType::Account => Some(PublicSearchScope::Main),
        CanonicalScopeType::Project => Some(PublicSearchScope::Project),
        CanonicalScopeType::AgentChat => Some(if project_agent_chat {
            PublicSearchScope::Project
        } else {
            PublicSearchScope::Main
        }),
        CanonicalScopeType::Task => None,
    }
}

/// Whether a scope may dispatch an ephemeral inquiry sub-agent.
///
/// Only an Agent Chat can: a Main Chat is a conversation a user is watching,
/// which is what makes an inquiry's cost and its run record legible. The
/// Account scope an inquiry itself runs under is deliberately excluded, and
/// that exclusion is what caps dispatch depth at one.
fn dispatches_inquiries(scope_type: CanonicalScopeType) -> bool {
    matches!(scope_type, CanonicalScopeType::AgentChat)
}

fn task_operations() -> (Vec<String>, Vec<String>) {
    (
        operation_registry::task_reads::IDS
            .iter()
            .map(|id| (*id).to_owned())
            .collect(),
        // The lead operations are listed for every Task session and
        // filtered by the evaluator like the rest: only a run whose principal
        // leads a root is offered them.
        operation_registry::task_records::IDS
            .iter()
            .chain(operation_registry::task_lead::IDS)
            .map(|id| (*id).to_owned())
            .collect(),
    )
}

fn filter_operations(
    scope_type: CanonicalScopeType,
    operations: &[String],
    allowed_permissions: &BTreeSet<String>,
    project_chat: ProjectChatToolContext,
    admitted: Option<&operation_registry::authority::EffectiveAuthority>,
) -> Vec<String> {
    operations
        .iter()
        .filter(|operation| {
            if let Some(spec) = operation_registry::READ_CATALOG
                .lookup(operation)
                .or_else(|| operation_registry::PROPOSAL_CATALOG.lookup(operation))
            {
                if let Some(admitted) = admitted {
                    return admitted.evaluate(spec).is_ok();
                }
                use operation_registry::authority::{
                    AuthorityFacts, EffectiveAuthority, Principal,
                };
                let principal = if scope_type == CanonicalScopeType::Task {
                    Principal::TaskAgent {
                        identity_id: String::new(),
                        execution_id: "projection".into(),
                        role: operation_registry::authority::TaskRole::Planner,
                        runtime_session_id: None,
                        lead_root_id: None,
                    }
                } else if scope_type == CanonicalScopeType::Project
                    || project_chat.is_project_agent_chat
                {
                    Principal::ProjectAgent {
                        identity_id: String::new(),
                        project_id: String::new(),
                    }
                } else {
                    Principal::MainAgent {
                        identity_id: String::new(),
                    }
                };
                return EffectiveAuthority::resolve(AuthorityFacts {
                    principal,
                    scope_type: scope_type_name(scope_type).to_owned(),
                    scope_id: String::new(),
                    profile_id: String::new(),
                    layers: vec![allowed_permissions.clone()],
                    binding_id: Some("projection".into()),
                    setup_required: project_chat.charter_setup_required,
                    active: true,
                })
                .evaluate(spec)
                .is_ok();
            }
            let descriptor =
                crate::operation_catalog::descriptor(scope_type, operation.as_str(), None);
            descriptor.is_exposed()
                && descriptor
                    .required_permission
                    .is_some_and(|permission| allowed_permissions.contains(permission))
        })
        .cloned()
        .collect()
}

/// Whether `origin` is an https origin outside this host's own network.
///
/// The authorization gate cannot resolve DNS — that is the transport's job at
/// connect time, pinned to the addresses it checked — so this refuses the
/// shapes that need no lookup: a non-https scheme, a literal private or
/// loopback address, and a hostname that names the local network.
fn public_https_origin(origin: &str) -> bool {
    let Ok(parsed) = url::Url::parse(origin) else {
        return false;
    };
    if parsed.scheme() != "https" {
        return false;
    }
    match parsed.host() {
        Some(url::Host::Domain(domain)) => !config::is_restricted_public_hostname(domain),
        Some(url::Host::Ipv4(address)) => !config::is_restricted_public_address(address.into()),
        Some(url::Host::Ipv6(address)) => !config::is_restricted_public_address(address.into()),
        None => false,
    }
}

fn required_string<'a>(arguments: &'a Value, field: &str) -> Result<&'a str, RuntimeError> {
    arguments
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeError::tool(format!("{field} must be a string")))
}

/// Refuse a server-derived field name or a prompt-injection field name
/// anywhere in the arguments. `project_id` is not in this guard: reads and
/// hand-path proposals take a real `project_id` argument.
fn reject_authority_overrides(arguments: &Value) -> Result<(), RuntimeError> {
    const PROMPT_INJECTION_FIELDS: &[&str] = &[
        "instruction",
        "instructions",
        "system_prompt",
        "policy_override",
        "tool_policy",
        "role",
    ];

    fn contains_forbidden(value: &Value) -> bool {
        match value {
            Value::Object(object) => object.iter().any(|(key, nested)| {
                crate::operation_catalog::SERVER_DERIVED_FIELDS.contains(&key.as_str())
                    || PROMPT_INJECTION_FIELDS.contains(&key.as_str())
                    || contains_forbidden(nested)
            }),
            Value::Array(values) => values.iter().any(contains_forbidden),
            _ => false,
        }
    }

    if contains_forbidden(arguments) {
        return Err(authority_override_refusal());
    }
    Ok(())
}

fn authority_override_refusal() -> RuntimeError {
    RuntimeError::tool("Forge orchestration scope and authority are server-derived")
}

/// Whether the read path admits this operation's arguments envelope.
///
/// The admitted nested names below are a second gate in front of the JSON
/// schema the model is shown. Exposed so the two can be pinned to each other
/// in a test: an operation advertised with arguments this refuses fails as an
/// opaque internal error, which is unusable for the model and invisible in
/// the log.
#[must_use]
pub fn admits_orchestration_read_argument(operation: &str, arguments: &Value) -> bool {
    validate_orchestration_read_arguments(operation, arguments).is_ok()
}

fn validate_orchestration_read_arguments(
    operation: &str,
    arguments: &Value,
) -> Result<(), RuntimeError> {
    let object = arguments.as_object().ok_or_else(|| {
        RuntimeError::tool("Forge orchestration read arguments must be an object")
    })?;
    if let Some(spec) = operation_registry::READ_CATALOG.lookup(operation) {
        if let Some(field) = object
            .keys()
            .find(|field| !matches!(field.as_str(), "operation" | "arguments"))
        {
            return Err(RuntimeError::tool(format!(
                "Forge orchestration read field `{field}` is not admitted"
            )));
        }
        return spec
            .validate_arguments(object.get("arguments").unwrap_or(&json!({})))
            .map_err(RuntimeError::tool);
    }
    Err(RuntimeError::tool(
        "Forge orchestration read operation has no registered contract",
    ))
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let value = value.chars().collect::<Vec<_>>();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;
    for token in pattern.chars() {
        let mut current = vec![false; value.len() + 1];
        if token == '*' {
            current[0] = previous[0];
        }
        for (index, character) in value.iter().enumerate() {
            current[index + 1] = if token == '*' {
                previous[index + 1] || current[index]
            } else {
                previous[index] && (token == '?' || token == *character)
            };
        }
        previous = current;
    }
    previous[value.len()]
}

fn workspace_relative_path(root: &str, path: &str) -> Result<String, RuntimeError> {
    let relative = Path::new(path)
        .strip_prefix(Path::new(root))
        .map_err(|_| RuntimeError::workspace("Task Workspace path is outside its root"))?;
    if relative.as_os_str().is_empty() {
        return Ok(".".to_owned());
    }
    let segments = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(segment) => segment.to_str(),
            Component::CurDir => None,
            _ => None,
        })
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return Err(RuntimeError::workspace(
            "Task Workspace path is not valid UTF-8",
        ));
    }
    Ok(segments.join("/"))
}

fn tool_error_outcome(value: Value) -> ToolOutcome {
    let mut outcome = ToolOutcome::json(value);
    outcome.is_error = true;
    outcome
}

fn validate_command_program(
    allowlist: &CommandAllowlist,
    program: &str,
) -> Result<(), RuntimeError> {
    if program.contains('/') || program.contains('\\') || !allowlist.allows(program) {
        return Err(RuntimeError::tool(format!(
            "`{program}` is not in this workspace's command allowlist; the owner configures it \
             under `commands` in the Forge config or `command_allowlist` in the Project settings"
        )));
    }
    Ok(())
}

fn scope_type_name(scope_type: CanonicalScopeType) -> &'static str {
    match scope_type {
        CanonicalScopeType::Account => "account",
        CanonicalScopeType::Project => "project",
        CanonicalScopeType::AgentChat => "agent_chat",
        CanonicalScopeType::Task => "task",
    }
}

/// Preserve a provider's typed Forge outcome as an in-band runtime result.
///
/// `RuntimeError` is reserved for malformed calls and runtime failures.  A
/// command/query service can instead return a safe, structured domain outcome
/// (for example a version conflict with an authorized retry snapshot).  The
/// model must receive that envelope unchanged with `is_error = true`, so it
/// can branch on `code` and typed corrective fields rather than parse prose.
fn provider_result_to_tool_outcome(
    result: Result<Value, AgentHostError>,
) -> Result<ToolOutcome, RuntimeError> {
    match result {
        Ok(value) => Ok(ToolOutcome::json(value)),
        Err(AgentHostError::StructuredOutcome(outcome)) => {
            // Every field in `OrchestrationOutcome` is JSON-compatible, so
            // this normally cannot fail.  Keep even the defensive fallback
            // in-band and redacted if a future field adds a fallible
            // serializer; a domain error must never become RuntimeError
            // prose at this boundary.
            let value = serde_json::to_value(outcome.as_ref()).unwrap_or_else(|_| {
                json!({
                    "code": "internal_failure",
                    "status": "failed",
                    "operation": outcome.operation,
                    "scope": outcome.scope,
                    "safe_message": "The command could not be completed.",
                    "correlation_id": outcome.correlation_id,
                    "replayed": false,
                })
            });
            Ok(ToolOutcome {
                value,
                content: Default::default(),
                is_error: true,
            })
        }
        Err(error) => Err(host_error_to_runtime(error)),
    }
}

fn host_error_to_runtime(error: AgentHostError) -> RuntimeError {
    match error {
        AgentHostError::Authority(message)
        | AgentHostError::Configuration(message)
        | AgentHostError::Unsupported(message) => RuntimeError::tool(message),
        AgentHostError::AgentPaused { .. } | AgentHostError::ProjectPaused { .. } => {
            RuntimeError::tool("Forge operation is paused")
        }
        AgentHostError::CredentialNotFound | AgentHostError::SessionNotFound => {
            RuntimeError::not_found("Forge runtime resource unavailable")
        }
        AgentHostError::VersionConflict => {
            RuntimeError::tool("Forge runtime resource changed; retry with the current version")
        }
        // Runtime carries operational feedback (validation conflicts, policy
        // rejections) the model needs verbatim to self-correct; only the
        // protected-persistence path stays opaque.
        AgentHostError::Runtime(message) => {
            RuntimeError::tool(format!("Forge tool provider failed: {message}"))
        }
        AgentHostError::RuntimeWithUsage { message, .. } => {
            RuntimeError::tool(format!("Forge tool provider failed: {message}"))
        }
        AgentHostError::TurnLimitReached { limit } => RuntimeError::tool(format!(
            "Forge tool provider reached its runtime {limit} limit"
        )),
        AgentHostError::StructuredOutcome(_) => {
            // ForgeScope read/propose consume this variant through
            // `provider_result_to_tool_outcome`.  Keep any unrelated call
            // site opaque if it ever receives one rather than serializing a
            // potentially sensitive domain payload into runtime prose.
            RuntimeError::tool("Forge tool provider returned a structured outcome")
        }
        AgentHostError::SessionRetired => RuntimeError::conflict("session journal is retired"),
        AgentHostError::ProtectedPersistence => RuntimeError::tool("Forge tool provider failed"),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "typed_tools/normalization_tests.rs"]
mod normalization_tests;

#[cfg(test)]
#[path = "typed_tools/registry_tests.rs"]
mod registry_tests;

#[cfg(test)]
#[path = "typed_tools/lead_tests.rs"]
mod lead_tests;

#[cfg(test)]
mod proposal_registry_tests;

#[cfg(test)]
mod public_origin_security_tests;
