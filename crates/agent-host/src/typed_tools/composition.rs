//! Scope-derived tool composition and runtime installation.

use super::*;

fn public_search_permission(
    scope_type: CanonicalScopeType,
    project_agent_chat: bool,
) -> &'static str {
    match (scope_type, project_agent_chat) {
        (CanonicalScopeType::Account, _) | (CanonicalScopeType::AgentChat, false) => {
            "propose_discovery"
        }
        (CanonicalScopeType::Project, true) | (CanonicalScopeType::Project, false) => {
            "read_project"
        }
        (CanonicalScopeType::AgentChat, true) => "read_agent_chat",
        (CanonicalScopeType::Task, _) => "__unknown_public_search_permission__",
    }
}

/// Whether this composition is an ephemeral inquiry sub-agent's own scope.
///
/// An inquiry runs under the Account scope with a scratch directory, which is
/// a shape nothing else uses: a Main Chat holds `account_scratch` too but is
/// an Agent Chat, and every other Account session denies filesystem access.
fn is_inquiry_scope(scope: &CanonicalScope) -> bool {
    scope.is_ephemeral_inquiry()
}

fn orchestration_surface(
    scope_type: CanonicalScopeType,
    project_agent_chat: bool,
) -> Option<OperationSurface> {
    match scope_type {
        CanonicalScopeType::Account => Some(OperationSurface::MainOrchestration),
        CanonicalScopeType::Project => Some(OperationSurface::ProjectOrchestration),
        CanonicalScopeType::AgentChat => Some(if project_agent_chat {
            OperationSurface::ProjectOrchestration
        } else {
            OperationSurface::MainOrchestration
        }),
        CanonicalScopeType::Task => None,
    }
}

fn orchestration_descriptor(
    surface: OperationSurface,
) -> (&'static str, &'static str, &'static str) {
    match surface {
        OperationSurface::MainOrchestration => (
            FORGE_MAIN_ORCHESTRATION_READ_TOOL,
            FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL,
            "the authenticated global Main Agent account/Chat scope",
        ),
        OperationSurface::ProjectOrchestration => (
            FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
            FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
            "the authenticated single-Project Project Agent scope",
        ),
        OperationSurface::Coordination => unreachable!("coordination has no typed descriptor"),
    }
}

fn bound_chat_tool_error(message: String, limit: usize) -> String {
    message.chars().take(limit).collect()
}

impl ScopeToolComposition {
    /// Composes one scope using a server-computed effective permission set.
    ///
    /// The set is computed by Forge after intersecting the identity, selected
    /// profile, membership, and canonical-scope ceilings.  Keeping it
    /// mandatory here prevents an exported host composition from silently
    /// falling back to role-only authority.
    pub fn for_scope_with_permissions(
        actor_identity_id: impl Into<String>,
        scope: CanonicalScope,
        task_role: Option<&str>,
        workspace_root: Option<&str>,
        allowed_permissions: &BTreeSet<String>,
        provider: Option<Arc<dyn ForgeToolProvider>>,
    ) -> Result<Self, AgentHostError> {
        Self::for_scope_with_permissions_and_project_chat(
            actor_identity_id,
            scope,
            task_role,
            workspace_root,
            allowed_permissions,
            false,
            provider,
        )
    }

    /// Compose a scope using an optional server-derived Project Agent Chat
    /// authority.  The default composition above deliberately does not infer
    /// chat kind from an opaque id.  Native execution passes this bit only
    /// after the protected store has joined the canonical chat row and its
    /// owning Project, so Main Chat cannot acquire Task proposals by sending
    /// a forged permission set or prompt.
    pub fn for_scope_with_permissions_and_project_chat(
        actor_identity_id: impl Into<String>,
        scope: CanonicalScope,
        task_role: Option<&str>,
        workspace_root: Option<&str>,
        allowed_permissions: &BTreeSet<String>,
        project_agent_chat: bool,
        provider: Option<Arc<dyn ForgeToolProvider>>,
    ) -> Result<Self, AgentHostError> {
        Self::for_scope_with_permissions_and_project_context(
            actor_identity_id,
            scope,
            task_role,
            workspace_root,
            allowed_permissions,
            ProjectChatToolContext {
                is_project_agent_chat: project_agent_chat,
                charter_setup_required: false,
            },
            provider,
            ScopeToolRuntime::default(),
        )
    }

    /// Compose a Project scope with the server-derived legacy adoption state.
    /// Setup mode has a deliberately smaller generic and typed catalog: the
    /// Project Agent can read the bounded current state, send a message, and
    /// draft the unapproved adoption Charter only.  This boolean must come
    /// from the protected Project record, never from model arguments.
    // Eight inputs, each a distinct server-derived authority: identity, scope,
    // Task role, workspace root, permissions, Project chat state, the Forge
    // operation provider, and the host's runtime inputs. Bundling them into
    // one struct would only move the list somewhere less checked.
    #[allow(clippy::too_many_arguments)]
    pub fn for_scope_with_permissions_and_project_context(
        actor_identity_id: impl Into<String>,
        scope: CanonicalScope,
        task_role: Option<&str>,
        workspace_root: Option<&str>,
        allowed_permissions: &BTreeSet<String>,
        project_chat: ProjectChatToolContext,
        provider: Option<Arc<dyn ForgeToolProvider>>,
        runtime: ScopeToolRuntime,
    ) -> Result<Self, AgentHostError> {
        scope.validate()?;
        let mut admitted_authority = provider
            .as_ref()
            .and_then(|provider| provider.admitted_authority())
            .cloned();
        let denial_provider = provider.clone();
        // `None` is the built-in set. An owner widens or replaces it in the
        // Forge config and per Project; the resolved list arrives with the
        // turn, so model input can never reach this.
        let command_allowlist = runtime
            .command_allowlist
            .clone()
            .unwrap_or_else(|| Arc::new(CommandAllowlist::builtin()));
        let actor_identity_id = actor_identity_id.into();
        if scope.scope_type == CanonicalScopeType::Task && admitted_authority.is_none() {
            use operation_registry::authority::{
                AuthorityFacts, EffectiveAuthority, Principal, TaskRole,
            };
            if let Some(role) = task_role.and_then(TaskRole::parse) {
                admitted_authority = Some(EffectiveAuthority::resolve(AuthorityFacts {
                    principal: Principal::TaskAgent {
                        identity_id: actor_identity_id.clone(),
                        execution_id: "projection".into(),
                        role,
                        runtime_session_id: None,
                        lead_root_id: None,
                    },
                    scope_type: "task".into(),
                    scope_id: scope.scope_id.clone(),
                    profile_id: String::new(),
                    layers: vec![allowed_permissions.clone()],
                    binding_id: None,
                    setup_required: false,
                    active: true,
                }));
            }
        }
        if actor_identity_id.trim().is_empty() {
            return Err(AgentHostError::Authority(
                "native tool composition requires a server-issued identity".to_owned(),
            ));
        }
        if matches!(scope.scope_type, CanonicalScopeType::Task)
            && workspace_root
                .filter(|root| !root.trim().is_empty())
                .is_none()
        {
            return Err(AgentHostError::Authority(
                "Task tool composition requires the host-issued workspace root".to_owned(),
            ));
        }
        // A Project Agent Chat may hold one disposable verification checkout so
        // it can exercise the delivered software itself. Every other non-Task
        // scope still receives no workspace at all.
        let project_verification = scope.scope_type == CanonicalScopeType::AgentChat
            && scope.workspace_access == WorkspaceAccess::ProjectVerify;
        if project_verification
            && workspace_root
                .filter(|root| !root.trim().is_empty())
                .is_none()
        {
            return Err(AgentHostError::Authority(
                "Project verification composition requires the host-issued checkout".to_owned(),
            ));
        }
        // The Main Agent, and every ephemeral inquiry sub-agent it dispatches,
        // may hold an account scratch directory for working notes and
        // findings. It is not a checkout: no repository is present in it, so
        // there is no route from this surface back to any repository.
        let account_scratch = matches!(
            scope.scope_type,
            CanonicalScopeType::Account | CanonicalScopeType::AgentChat
        ) && scope.workspace_access == WorkspaceAccess::AccountScratch;
        if account_scratch
            && workspace_root
                .filter(|root| !root.trim().is_empty())
                .is_none()
        {
            return Err(AgentHostError::Authority(
                "account scratch composition requires the host-issued directory".to_owned(),
            ));
        }
        if !matches!(scope.scope_type, CanonicalScopeType::Task)
            && !project_verification
            && !account_scratch
            && workspace_root.is_some()
        {
            return Err(AgentHostError::Authority(
                "non-Task tool composition cannot receive a workspace root".to_owned(),
            ));
        }

        let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
        let mut coverage_set = BTreeSet::new();
        let mut custom_permissions = BTreeSet::new();

        match scope.scope_type {
            CanonicalScopeType::Task => {
                let role = TaskToolRole::parse(&scope, task_role)?;
                let root = workspace_root.expect("validated Task workspace root");
                let task_read_allowed = allowed_permissions.contains("task_read");
                let task_write_allowed = allowed_permissions.contains("task_write");
                if task_read_allowed {
                    tools.push(Arc::new(TaskReadTool));
                    tools.push(Arc::new(TaskListTool));
                    coverage_set.insert(Permission::FsRead);
                }
                match role {
                    TaskToolRole::Worker => {
                        if task_write_allowed {
                            tools.push(Arc::new(TaskWriteTool));
                            tools.push(Arc::new(TaskCommandTool {
                                environment: runtime.environment.clone(),
                                allowlist: Arc::clone(&command_allowlist),
                                observer: None,
                                command_dir: None,
                            }));
                            coverage_set.insert(Permission::FsWrite);
                            coverage_set.insert(Permission::ProcessSpawn);
                        }
                    }
                    TaskToolRole::Reviewer => {
                        if task_read_allowed {
                            tools.push(Arc::new(TaskValidateTool {
                                environment: runtime.environment.clone(),
                            }));
                            coverage_set.insert(Permission::ProcessSpawn);
                        }
                    }
                    // The planner surface is the read tool above only: no
                    // writes, no command spawn, no validation process.
                    TaskToolRole::Planner => {}
                }
                // Reading public documentation is part of writing code against
                // a dependency. It is the same authority the chat agents get,
                // bounded to GET over https by the transport, and gated on the
                // Task read permission so a Task that may not read its own
                // record cannot reach the network either.
                if task_read_allowed {
                    if let Some(transport) = runtime.fetch_transport.clone() {
                        tools.push(Arc::new(FetchTool::new(transport)));
                        coverage_set.insert(Permission::NetHttp);
                    }
                }
                if let Some(provider) = provider {
                    let (read_operations, propose_operations) = task_operations();
                    let read_operations = filter_operations(
                        scope.scope_type,
                        &read_operations,
                        allowed_permissions,
                        project_chat,
                        admitted_authority.as_ref(),
                    );
                    let propose_operations = filter_operations(
                        scope.scope_type,
                        &propose_operations,
                        allowed_permissions,
                        project_chat,
                        admitted_authority.as_ref(),
                    );
                    if !read_operations.is_empty() {
                        tools.push(Arc::new(ForgeScopeReadTool::new(
                            actor_identity_id.clone(),
                            scope.clone(),
                            read_operations,
                            provider.clone(),
                        )));
                        custom_permissions.insert(Permission::other(FORGE_SCOPE_READ_PERMISSION));
                    }
                    if !propose_operations.is_empty() {
                        tools.push(Arc::new(ForgeScopeProposeTool::new(
                            actor_identity_id.clone(),
                            scope.clone(),
                            propose_operations,
                            provider,
                        )));
                        custom_permissions
                            .insert(Permission::other(FORGE_SCOPE_PROPOSE_PERMISSION));
                    }
                }
                let _ = root;
            }
            CanonicalScopeType::Account
            | CanonicalScopeType::Project
            | CanonicalScopeType::AgentChat => {
                // Verification is read-write-and-run, but only ever against
                // the disposable checkout: this Agent checks the delivered
                // software, it does not author it. There is no commit or push
                // route, so nothing it does here can reach the repository.
                if project_verification {
                    let root = workspace_root.expect("validated verification checkout");
                    tools.push(Arc::new(TaskReadTool));
                    tools.push(Arc::new(TaskListTool));
                    tools.push(Arc::new(TaskCommandTool {
                        environment: runtime.environment.clone(),
                        allowlist: Arc::clone(&command_allowlist),
                        observer: provider.clone().map(|provider| CommandObserver {
                            actor_identity_id: actor_identity_id.clone(),
                            scope: scope.clone(),
                            provider,
                        }),
                        command_dir: Some(PROJECT_VERIFICATION_CHECKOUT_DIR),
                    }));
                    tools.push(Arc::new(TaskWriteTool));
                    coverage_set.insert(Permission::FsRead);
                    coverage_set.insert(Permission::ProcessSpawn);
                    coverage_set.insert(Permission::FsWrite);
                    let _ = root;
                }
                // Account scratch is read/write/run inside a plain directory.
                // Commands run at the scratch root because there is no
                // checkout subdirectory to confine them to.
                if account_scratch {
                    let root = workspace_root.expect("validated account scratch directory");
                    tools.push(Arc::new(TaskReadTool));
                    tools.push(Arc::new(TaskListTool));
                    tools.push(Arc::new(TaskWriteTool));
                    tools.push(Arc::new(TaskCommandTool {
                        environment: runtime.environment.clone(),
                        allowlist: Arc::clone(&command_allowlist),
                        observer: None,
                        command_dir: None,
                    }));
                    coverage_set.insert(Permission::FsRead);
                    coverage_set.insert(Permission::FsWrite);
                    coverage_set.insert(Permission::ProcessSpawn);
                    let _ = root;
                }
                if let Some(provider) = provider {
                    let (read_operations, propose_operations) = non_task_operations(
                        scope.scope_type,
                        project_chat.is_project_agent_chat,
                        project_chat.charter_setup_required,
                    );
                    let read_operations = filter_operations(
                        scope.scope_type,
                        &read_operations,
                        allowed_permissions,
                        project_chat,
                        admitted_authority.as_ref(),
                    );
                    let propose_operations = filter_operations(
                        scope.scope_type,
                        &propose_operations,
                        allowed_permissions,
                        project_chat,
                        admitted_authority.as_ref(),
                    );
                    if !read_operations.is_empty() {
                        tools.push(Arc::new(ForgeScopeReadTool::new(
                            actor_identity_id.clone(),
                            scope.clone(),
                            read_operations,
                            provider.clone(),
                        )));
                        custom_permissions.insert(Permission::other(FORGE_SCOPE_READ_PERMISSION));
                    }
                    if !propose_operations.is_empty() {
                        tools.push(Arc::new(ForgeScopeProposeTool::new(
                            actor_identity_id.clone(),
                            scope.clone(),
                            propose_operations,
                            provider.clone(),
                        )));
                        custom_permissions
                            .insert(Permission::other(FORGE_SCOPE_PROPOSE_PERMISSION));
                    }

                    // `fetch` reaches the public web directly, which is the
                    // same authority class as the search tool below, so it is
                    // gated on the same permission. Unlike search it needs no
                    // configured endpoint: the transport is the host's.
                    if let Some(transport) = runtime.fetch_transport.clone() {
                        let fetch_permission = public_search_permission(
                            scope.scope_type,
                            project_chat.is_project_agent_chat,
                        );
                        if allowed_permissions.contains(fetch_permission) {
                            tools.push(Arc::new(FetchTool::new(transport)));
                            coverage_set.insert(Permission::NetHttp);
                        }
                    }
                    if let Some(search_scope) =
                        public_search_scope(scope.scope_type, project_chat.is_project_agent_chat)
                    {
                        let search_permission = public_search_permission(
                            scope.scope_type,
                            project_chat.is_project_agent_chat,
                        );
                        if provider.public_search_configured()
                            && allowed_permissions.contains(search_permission)
                        {
                            tools.push(Arc::new(ForgePublicWebSearchTool::new(
                                actor_identity_id.clone(),
                                scope.clone(),
                                search_scope,
                                provider.clone(),
                            )));
                            custom_permissions
                                .insert(Permission::other(FORGE_SCOPE_READ_PERMISSION));
                        }
                    }

                    // Orchestration is a separate typed surface.  It is
                    // deliberately not folded into the generic coordination
                    // tool: Main and Project scopes have different operation
                    // catalogs, and the descriptor itself must tell the
                    // model which authenticated scope it is operating in.
                    if let Some(surface) =
                        orchestration_surface(scope.scope_type, project_chat.is_project_agent_chat)
                    {
                        let orchestration_reads = operation_names_for_surface(
                            surface,
                            project_chat.charter_setup_required,
                            OperationExposure::TypedRead,
                        );
                        let orchestration_proposals = operation_names_for_surface(
                            surface,
                            project_chat.charter_setup_required,
                            OperationExposure::TypedProposal,
                        );
                        let orchestration_reads = filter_operations(
                            scope.scope_type,
                            &orchestration_reads,
                            allowed_permissions,
                            project_chat,
                            admitted_authority.as_ref(),
                        );
                        // The depth cap. An inquiry sub-agent runs under the
                        // Account scope, so withholding the dispatch
                        // operation there -- rather than asking a model not
                        // to use it -- makes recursion structurally
                        // impossible: only a Main Agent Chat can ever start
                        // an inquiry.
                        let orchestration_reads = if dispatches_inquiries(scope.scope_type) {
                            orchestration_reads
                        } else {
                            orchestration_reads
                                .into_iter()
                                .filter(|operation| operation != MAIN_INQUIRY_RUN_OPERATION)
                                .collect()
                        };
                        let orchestration_proposals = filter_operations(
                            scope.scope_type,
                            &orchestration_proposals,
                            allowed_permissions,
                            project_chat,
                            admitted_authority.as_ref(),
                        );
                        // An inquiry reads and reports; it never proposes.
                        // Withholding the whole proposal surface is what
                        // makes that true structurally, and permissions
                        // alone would not: `genesis.start` is gated on
                        // `propose_discovery`, the same permission a
                        // sub-agent needs for public research, so a
                        // permission-only boundary would hand a research
                        // sub-agent Product Genesis on its dispatcher's
                        // account.
                        let orchestration_proposals = if is_inquiry_scope(&scope) {
                            Vec::new()
                        } else {
                            orchestration_proposals
                        };
                        let (read_name, propose_name, scope_label) =
                            orchestration_descriptor(surface);
                        if !orchestration_reads.is_empty() {
                            tools.push(Arc::new(ForgeScopeReadTool::named(
                                actor_identity_id.clone(),
                                scope.clone(),
                                orchestration_reads,
                                provider.clone(),
                                read_name,
                                scope_label,
                            )));
                            custom_permissions
                                .insert(Permission::other(FORGE_SCOPE_READ_PERMISSION));
                        }
                        if !orchestration_proposals.is_empty() {
                            tools.push(Arc::new(ForgeScopeProposeTool::named(
                                actor_identity_id.clone(),
                                scope.clone(),
                                orchestration_proposals,
                                provider,
                                propose_name,
                                scope_label,
                            )));
                            custom_permissions
                                .insert(Permission::other(FORGE_SCOPE_PROPOSE_PERMISSION));
                        }
                    }
                }
            }
        }
        // The runtime seals and caches ToolSpec at registration. It cannot
        // withdraw an operation between model calls. Keep that catalog and
        // suppress repeat evaluations in the Forge composition instead.
        if let Some(provider) = denial_provider {
            let denials = Arc::new(Mutex::new(BTreeMap::new()));
            tools = tools
                .into_iter()
                .map(|inner| {
                    if matches!(
                        inner.spec().name.as_str(),
                        "forge_scope_read"
                            | "forge_scope_propose"
                            | FORGE_MAIN_ORCHESTRATION_READ_TOOL
                            | FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL
                            | FORGE_PROJECT_ORCHESTRATION_READ_TOOL
                            | FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL
                            | FORGE_PUBLIC_WEB_SEARCH_TOOL
                    ) {
                        Arc::new(TerminalDenialTool {
                            inner,
                            denials: Arc::clone(&denials),
                            provider: Arc::clone(&provider),
                            actor_identity_id: actor_identity_id.clone(),
                            scope: scope.clone(),
                        }) as Arc<dyn Tool>
                    } else {
                        inner
                    }
                })
                .collect();
        }
        coverage_set.extend(custom_permissions);
        let coverage: PermissionSet = coverage_set.into_iter().collect();
        let security_check = Arc::new(ForgeScopeSecurityCheck {
            id: SecurityCheckId::new(format!(
                "forge-scope:{}:{}",
                scope_type_name(scope.scope_type),
                scope.scope_id
            )),
            revision: SecurityCheckRevision::new("forge-native-tools-v1"),
            coverage: coverage.clone(),
            workspace_root: workspace_root.map(str::to_owned),
        });
        Ok(Self {
            tools,
            security_check,
            coverage,
            actor_identity_id,
            scope,
        })
    }

    /// Returns the exact advertised names in deterministic order.
    pub fn tool_names(&self) -> Vec<String> {
        let mut names = self
            .tools
            .iter()
            .map(|tool| tool.spec().name)
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    /// Returns the exact tool registry entries for inspection or composition.
    pub fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.clone()
    }

    /// Returns the host-assigned typed permission coverage.
    pub fn coverage(&self) -> PermissionSet {
        self.coverage.clone()
    }

    /// Applies this composition to a RuntimeBuilder.
    pub fn apply(self, builder: RuntimeBuilder) -> RuntimeBuilder {
        builder.tools(self.tools).security_check(
            self.security_check,
            SecurityCheckMode::Authoritative,
            self.coverage,
            ActionClass::new("forge-native-scope"),
        )
    }

    /// The identity captured by the host composition.  This is informational
    /// and useful for setting RuntimeBuilder's security subject; tools never
    /// read it from model arguments.
    pub fn actor_identity_id(&self) -> &str {
        &self.actor_identity_id
    }

    /// The canonical scope captured by the host composition.
    pub fn scope(&self) -> &CanonicalScope {
        &self.scope
    }

    /// Invokes one already-composed Forge tool for the CLI chat callback.
    ///
    /// CLI chat has no workspace and no interactive approval channel.  Keep
    /// this path host-owned: the model supplies only the tool name and
    /// arguments, while identity, tenant, check-set revision, workspace, and
    /// permission coverage all come from this immutable composition.
    pub async fn invoke_denied_chat_tool(
        &self,
        session_id: &str,
        turn_id: &str,
        call_id: &str,
        tool_name: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        const MAX_CHAT_TOOL_ERROR_CHARS: usize = 4 * 1024;

        if self.scope.scope_type != CanonicalScopeType::AgentChat
            || self.scope.workspace_access != WorkspaceAccess::Deny
        {
            return Err(AgentHostError::Authority(
                "CLI chat tool invocation requires a denied Agent Chat scope".to_owned(),
            ));
        }
        if [session_id, turn_id, call_id]
            .into_iter()
            .any(|id| id.trim().is_empty())
        {
            return Err(AgentHostError::Authority(
                "CLI chat tool invocation requires server-issued identifiers".to_owned(),
            ));
        }

        let tool = self
            .tools
            .iter()
            .find(|tool| tool.spec().name == tool_name)
            .cloned()
            .ok_or_else(|| {
                AgentHostError::Unsupported("Forge chat tool is not available".to_owned())
            })?;
        // Freeze the selected spec with the same registry/validator used by
        // native execution. Preparation receives only normalized, validated
        // arguments; failures never retry the raw provider value.
        let map_error = |error: RuntimeError| {
            AgentHostError::Runtime(bound_chat_tool_error(
                error.to_string(),
                MAX_CHAT_TOOL_ERROR_CHARS,
            ))
        };
        let mut registry = agent_runtime::tool::ToolRegistry::new();
        registry.register(tool.clone()).map_err(map_error)?;
        let registry = registry.seal();
        let spec = registry.spec(tool_name).expect("registered CLI tool");
        let arguments = tool.normalize_arguments(arguments).map_err(map_error)?;
        registry
            .validate_arguments(tool_name, &arguments)
            .map_err(map_error)?;

        let session = SessionId::new(session_id);
        let turn = TurnId::new(turn_id);
        let call = ToolCallId::new(call_id);
        let request = RequestId::new(format!("forge-chat:{session_id}:{turn_id}:{call_id}"));
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let cancel = Cancellation::new();
        let deadline = Deadline::never();
        let workspace: Arc<dyn Workspace> = Arc::new(DenyAllWorkspace);

        let preparation = PreparationContext {
            session: session.clone(),
            turn: Some(turn.clone()),
            call_id: call.clone(),
            request: request.clone(),
            workspace: workspace.clone(),
            clock: clock.clone(),
            cancel: cancel.clone(),
            deadline,
        };
        let prepared = tool
            .prepare(arguments, &preparation)
            .await
            .map_err(|error| {
                AgentHostError::Runtime(bound_chat_tool_error(
                    error.to_string(),
                    MAX_CHAT_TOOL_ERROR_CHARS,
                ))
            })?;
        if !prepared.verify_fingerprint()
            || !prepared
                .required_permissions()
                .is_subset(&spec.permission_upper_bound)
            || !prepared.required_permissions().is_subset(&self.coverage)
        {
            return Err(AgentHostError::Authority(
                "Forge chat tool preparation exceeded its composed authority".to_owned(),
            ));
        }

        if tool.supports_interaction()
            || tool
                .interaction_request(
                    &prepared,
                    InteractionOrigin::new(session.clone(), turn.clone(), call.clone()),
                    deadline,
                )
                .map_err(|error| {
                    AgentHostError::Runtime(bound_chat_tool_error(
                        error.to_string(),
                        MAX_CHAT_TOOL_ERROR_CHARS,
                    ))
                })?
                .is_some()
        {
            return Err(AgentHostError::Unsupported(
                "interactive Forge tools are unavailable in CLI chat".to_owned(),
            ));
        }

        let security_context = SecurityContext::new(
            SecuritySubject::new(self.actor_identity_id.clone()),
            session.clone(),
            TenantId::new(self.scope.scope_id.clone()),
            CheckSetRevision::new(self.security_check.revision().as_str()),
        );
        let authorization = AuthorizationRequest::new(
            security_context,
            SecurityAction::new("tool.invoke"),
            prepared.resource().clone(),
            prepared.required_permissions().clone(),
            deadline,
            SecurityEvidence::new(TrustClass::ExternalContent, prepared.fingerprint().clone()),
        );
        if !matches!(
            self.security_check.evaluate(&authorization, &cancel).await,
            SecurityCheckOutcome::Allow { .. }
        ) {
            return Err(AgentHostError::Authority(
                "Forge chat tool authorization was denied".to_owned(),
            ));
        }

        let invocation = InvocationContext {
            session,
            turn: Some(turn),
            call_id: call,
            request,
            workspace,
            clock,
            cancel,
            deadline,
            output_limit: 128 * 1024,
        };
        let outcome = tool.invoke(prepared, &invocation).await.map_err(|error| {
            AgentHostError::Runtime(bound_chat_tool_error(
                error.to_string(),
                MAX_CHAT_TOOL_ERROR_CHARS,
            ))
        })?;
        if outcome.is_error {
            let message = match &outcome.value {
                Value::String(message) if !message.trim().is_empty() => message.clone(),
                value => value.to_string(),
            };
            return Err(AgentHostError::Runtime(bound_chat_tool_error(
                message,
                MAX_CHAT_TOOL_ERROR_CHARS,
            )));
        }
        Ok(outcome.value)
    }

    pub(crate) fn filter_results(mut self, filter: Arc<dyn ToolResultFilter>) -> Self {
        self.tools = self
            .tools
            .into_iter()
            .map(|inner| {
                Arc::new(FilteredTool {
                    inner,
                    filter: filter.clone(),
                }) as Arc<dyn Tool>
            })
            .collect();
        self
    }

    /// Wraps every composed tool so `observer` sees the exact call id and
    /// result immediately before it returns to the runtime.
    ///
    /// `RuntimeEvent::ToolCallCompleted` (the runtime-event boundary the host
    /// observes) carries only `is_error`; a typed Forge command result is
    /// otherwise lost before it reaches the durable log, runtime event, and
    /// UI tool card. This reunites them without changing tool behavior:
    /// `observer` must stay synchronous and must not panic.
    #[must_use]
    pub fn observe_results(mut self, observer: ToolResultObserver) -> Self {
        self.tools = self
            .tools
            .into_iter()
            .map(|tool| -> Arc<dyn Tool> {
                Arc::new(ObservedTool {
                    inner: tool,
                    observer: Arc::clone(&observer),
                })
            })
            .collect();
        self
    }
}
